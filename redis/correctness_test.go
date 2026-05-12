/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Swytch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Swytch. If not, see <https://www.gnu.org/licenses/>.
 */

package redis

// End-to-end correctness tests for Redis protocol implementation.
// These tests verify protocol compliance with Redis.

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/swytchdb/swytch/effects"
)

// redisClient wraps a connection for test convenience
type redisClient struct {
	conn   net.Conn
	reader *bufio.Reader
	t      *testing.T
}

func newRedisClient(t *testing.T, addr string) *redisClient {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	return &redisClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		t:      t,
	}
}

func (c *redisClient) close() {
	c.conn.Close()
}

// sendCommand sends a RESP array command
func (c *redisClient) sendCommand(args ...string) {
	var cmd strings.Builder
	cmd.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, arg := range args {
		cmd.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}
	_, err := c.conn.Write([]byte(cmd.String()))
	if err != nil {
		c.t.Fatalf("failed to send command: %v", err)
	}
}

// sendInline sends an inline command
func (c *redisClient) sendInline(cmd string) {
	_, err := fmt.Fprintf(c.conn, "%s\r\n", cmd)
	if err != nil {
		c.t.Fatalf("failed to send inline command: %v", err)
	}
}

// readLine reads a single line
func (c *redisClient) readLine() string {
	line, err := c.reader.ReadString('\n')
	if err != nil {
		c.t.Fatalf("failed to read line: %v", err)
	}
	return strings.TrimSuffix(line, "\r\n")
}

// readReply reads a full RESP reply and returns it as a string
func (c *redisClient) readReply() string {
	line := c.readLine()
	if len(line) == 0 {
		c.t.Fatal("empty reply")
	}

	switch line[0] {
	case '+': // Simple string
		return line[1:]
	case '-': // Error
		return line // Keep the - prefix for error checking
	case ':': // Integer
		return line[1:]
	case '$': // Bulk string
		length, err := strconv.Atoi(line[1:])
		if err != nil {
			c.t.Fatalf("invalid bulk string length: %v", err)
		}
		if length == -1 {
			return "(nil)"
		}
		// Read the data + CRLF
		data := make([]byte, length+2)
		_, err = io.ReadFull(c.reader, data)
		if err != nil {
			c.t.Fatalf("failed to read bulk string: %v", err)
		}
		return string(data[:length])
	case '*': // Array
		count, err := strconv.Atoi(line[1:])
		if err != nil {
			c.t.Fatalf("invalid array count: %v", err)
		}
		if count == -1 {
			return "(nil)"
		}
		var parts []string
		for range count {
			parts = append(parts, c.readReply())
		}
		return strings.Join(parts, ",")
	case '%': // RESP3 Map
		count, err := strconv.Atoi(line[1:])
		if err != nil {
			c.t.Fatalf("invalid map count: %v", err)
		}
		var parts []string
		for range count {
			key := c.readReply()
			val := c.readReply()
			parts = append(parts, key+"="+val)
		}
		return "map{" + strings.Join(parts, ",") + "}"
	case '~': // RESP3 Set
		count, err := strconv.Atoi(line[1:])
		if err != nil {
			c.t.Fatalf("invalid set count: %v", err)
		}
		var parts []string
		for range count {
			parts = append(parts, c.readReply())
		}
		return "set{" + strings.Join(parts, ",") + "}"
	case '#': // RESP3 Boolean
		if line[1] == 't' {
			return "true"
		}
		return "false"
	case ',': // RESP3 Double
		return line[1:]
	case '_': // RESP3 Null
		return "(nil)"
	case '!': // RESP3 Blob Error
		length, err := strconv.Atoi(line[1:])
		if err != nil {
			c.t.Fatalf("invalid blob error length: %v", err)
		}
		data := make([]byte, length+2)
		_, err = io.ReadFull(c.reader, data)
		if err != nil {
			c.t.Fatalf("failed to read blob error: %v", err)
		}
		return "-" + string(data[:length])
	default:
		c.t.Fatalf("unknown reply type: %c in %q", line[0], line)
		return ""
	}
}

// readIntReply reads an integer reply
func (c *redisClient) readIntReply() int64 {
	line := c.readLine()
	if len(line) == 0 || line[0] != ':' {
		c.t.Fatalf("expected integer reply, got %q", line)
	}
	val, err := strconv.ParseInt(line[1:], 10, 64)
	if err != nil {
		c.t.Fatalf("invalid integer: %v", err)
	}
	return val
}

// Helper methods for common commands

func (c *redisClient) ping() string {
	c.sendCommand("PING")
	return c.readReply()
}

func (c *redisClient) set(key, value string) string {
	c.sendCommand("SET", key, value)
	return c.readReply()
}

func (c *redisClient) setEx(key string, seconds int, value string) string {
	c.sendCommand("SET", key, value, "EX", strconv.Itoa(seconds))
	return c.readReply()
}

func (c *redisClient) setPx(key string, milliseconds int, value string) string {
	c.sendCommand("SET", key, value, "PX", strconv.Itoa(milliseconds))
	return c.readReply()
}

func (c *redisClient) setNX(key, value string) string {
	c.sendCommand("SETNX", key, value)
	return c.readReply()
}

func (c *redisClient) get(key string) string {
	c.sendCommand("GET", key)
	return c.readReply()
}

func (c *redisClient) del(keys ...string) int64 {
	args := append([]string{"DEL"}, keys...)
	c.sendCommand(args...)
	return c.readIntReply()
}

func (c *redisClient) exists(keys ...string) int64 {
	args := append([]string{"EXISTS"}, keys...)
	c.sendCommand(args...)
	return c.readIntReply()
}

func (c *redisClient) incr(key string) string {
	c.sendCommand("INCR", key)
	return c.readReply()
}

func (c *redisClient) incrBy(key string, delta int64) string {
	c.sendCommand("INCRBY", key, strconv.FormatInt(delta, 10))
	return c.readReply()
}

func (c *redisClient) decr(key string) string {
	c.sendCommand("DECR", key)
	return c.readReply()
}

func (c *redisClient) decrBy(key string, delta int64) string {
	c.sendCommand("DECRBY", key, strconv.FormatInt(delta, 10))
	return c.readReply()
}

func (c *redisClient) expire(key string, seconds int) int64 {
	c.sendCommand("EXPIRE", key, strconv.Itoa(seconds))
	return c.readIntReply()
}

func (c *redisClient) ttl(key string) int64 {
	c.sendCommand("TTL", key)
	return c.readIntReply()
}

func (c *redisClient) pttl(key string) int64 {
	c.sendCommand("PTTL", key)
	return c.readIntReply()
}

func (c *redisClient) persist(key string) int64 {
	c.sendCommand("PERSIST", key)
	return c.readIntReply()
}

func (c *redisClient) appendCmd(key, value string) int64 {
	c.sendCommand("APPEND", key, value)
	return c.readIntReply()
}

func (c *redisClient) strlen(key string) int64 {
	c.sendCommand("STRLEN", key)
	return c.readIntReply()
}

func (c *redisClient) mget(keys ...string) string {
	args := append([]string{"MGET"}, keys...)
	c.sendCommand(args...)
	return c.readReply()
}

func (c *redisClient) mset(kvs ...string) string {
	args := append([]string{"MSET"}, kvs...)
	c.sendCommand(args...)
	return c.readReply()
}

func (c *redisClient) lpush(key string, values ...string) int64 {
	args := append([]string{"LPUSH", key}, values...)
	c.sendCommand(args...)
	return c.readIntReply()
}

func (c *redisClient) rpush(key string, values ...string) int64 {
	args := append([]string{"RPUSH", key}, values...)
	c.sendCommand(args...)
	return c.readIntReply()
}

func (c *redisClient) lpop(key string) string {
	c.sendCommand("LPOP", key)
	return c.readReply()
}

func (c *redisClient) rpop(key string) string {
	c.sendCommand("RPOP", key)
	return c.readReply()
}

func (c *redisClient) llen(key string) int64 {
	c.sendCommand("LLEN", key)
	return c.readIntReply()
}

func (c *redisClient) lrange(key string, start, stop int) string {
	c.sendCommand("LRANGE", key, strconv.Itoa(start), strconv.Itoa(stop))
	return c.readReply()
}

func (c *redisClient) lindex(key string, index int) string {
	c.sendCommand("LINDEX", key, strconv.Itoa(index))
	return c.readReply()
}

func (c *redisClient) hset(key string, fieldValues ...string) int64 {
	args := append([]string{"HSET", key}, fieldValues...)
	c.sendCommand(args...)
	return c.readIntReply()
}

func (c *redisClient) hget(key, field string) string {
	c.sendCommand("HGET", key, field)
	return c.readReply()
}

func (c *redisClient) hdel(key string, fields ...string) int64 {
	args := append([]string{"HDEL", key}, fields...)
	c.sendCommand(args...)
	return c.readIntReply()
}

func (c *redisClient) hexists(key, field string) int64 {
	c.sendCommand("HEXISTS", key, field)
	return c.readIntReply()
}

func (c *redisClient) hlen(key string) int64 {
	c.sendCommand("HLEN", key)
	return c.readIntReply()
}

func (c *redisClient) hgetall(key string) string {
	c.sendCommand("HGETALL", key)
	return c.readReply()
}

func (c *redisClient) hkeys(key string) string {
	c.sendCommand("HKEYS", key)
	return c.readReply()
}

func (c *redisClient) hvals(key string) string {
	c.sendCommand("HVALS", key)
	return c.readReply()
}

func (c *redisClient) hincrby(key, field string, delta int64) string {
	c.sendCommand("HINCRBY", key, field, strconv.FormatInt(delta, 10))
	return c.readReply()
}

func (c *redisClient) flushdb() string {
	c.sendCommand("FLUSHDB")
	return c.readReply()
}

func (c *redisClient) flushall() string {
	c.sendCommand("FLUSHALL")
	return c.readReply()
}

func (c *redisClient) dbsize() int64 {
	c.sendCommand("DBSIZE")
	return c.readIntReply()
}

func (c *redisClient) selectDB(index int) string {
	c.sendCommand("SELECT", strconv.Itoa(index))
	return c.readReply()
}

func (c *redisClient) swapDB(db1, db2 int) string {
	c.sendCommand("SWAPDB", strconv.Itoa(db1), strconv.Itoa(db2))
	return c.readReply()
}

func (c *redisClient) keys(pattern string) string {
	c.sendCommand("KEYS", pattern)
	return c.readReply()
}

func (c *redisClient) typeCmd(key string) string {
	c.sendCommand("TYPE", key)
	return c.readReply()
}

func (c *redisClient) psetex(key string, milliseconds int, value string) string {
	c.sendCommand("PSETEX", key, strconv.Itoa(milliseconds), value)
	return c.readReply()
}

// sortCSV sorts a comma-separated string for order-independent comparison.
func sortCSV(s string) string {
	if s == "" {
		return s
	}
	parts := strings.Split(s, ",")
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// startRedisTestServer starts a Redis server and returns its address
func startRedisTestServer(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	cfg := ServerConfig{
		Address:      "127.0.0.1:0",
		NumDatabases: 16,
	}

	cfg.CapacityPerDB = 10000

	eng := effects.NewTestEngine()

	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	if h, ok := server.handler.(*Handler); ok {
		h.engine = eng
		subs := h.dbManager.Subscriptions
		eng.OnKeyDataAdded = func(key string) { subs.Notify([]byte(key)) }
		eng.OnKeyDeleted = func(key string) { subs.NotifyAllWaiters([]byte(key)) }
		eng.OnFlushAll = func() { subs.NotifyAll() }
	}

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	return server.Addr().String(), func() { server.Stop() }
}

// startRedisTestServerWithEffects starts a test server with a standalone effects engine.
func startRedisTestServerWithEffects(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	eng := effects.NewTestEngine()

	cfg := ServerConfig{
		Address:       "127.0.0.1:0",
		NumDatabases:  16,
		CapacityPerDB: 10000,
	}

	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	if h, ok := server.handler.(*Handler); ok {
		h.engine = eng

		// Wire notification callbacks from effects engine to subscription manager
		subs := h.dbManager.Subscriptions
		eng.OnKeyDataAdded = func(key string) {
			subs.Notify([]byte(key))
		}
		eng.OnKeyDeleted = func(key string) {
			subs.NotifyAllWaiters([]byte(key))
		}
		eng.OnFlushAll = func() {
			subs.NotifyAll()
		}
	}

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	return server.Addr().String(), func() { server.Stop() }
}

// =============================================================================
// Connection Tests
// =============================================================================

func TestCorrectness_Ping(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		if resp := c.ping(); resp != "PONG" {
			t.Errorf("expected PONG, got %q", resp)
		}
	}
}

func TestCorrectness_PingWithArg(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.sendCommand("PING", "hello")
		resp := c.readReply()
		if resp != "hello" {
			t.Errorf("expected hello, got %q", resp)
		}
	}
}

func TestCorrectness_Echo(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.sendCommand("ECHO", "Hello World")
		resp := c.readReply()
		if resp != "Hello World" {
			t.Errorf("expected 'Hello World', got %q", resp)
		}
	}
}

func TestCorrectness_Select(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// SELECT succeeds but pins to DB 0 — effects engine disables multi-DB
		if resp := c.selectDB(1); resp != "OK" {
			t.Errorf("expected OK, got %q", resp)
		}

		// Store after SELECT 1 — should still be in DB 0
		c.set("db1key", "value1")

		// SELECT 0 — still DB 0, key should be visible
		c.selectDB(0)
		if val := c.get("db1key"); val != "value1" {
			t.Errorf("effects mode pins to DB 0, expected value1, got %q", val)
		}

		// Select invalid DB
		c.sendCommand("SELECT", "999")
		resp := c.readReply()
		if !strings.HasPrefix(resp, "-ERR") {
			t.Errorf("expected error for invalid DB, got %q", resp)
		}
	}
}

func TestCorrectness_Quit(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		fmt.Fprint(conn, "*1\r\n$4\r\nQUIT\r\n")

		// Connection should be closed
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 1)
		n, _ := conn.Read(buf)
		if n > 0 {
			// May get +OK before close, try again
			conn.SetReadDeadline(time.Now().Add(1 * time.Second))
			n, err = conn.Read(buf)
			if n != 0 || err == nil {
				t.Error("connection should be closed after quit")
			}
		}
	}
}

func TestCorrectness_InlineCommand(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Send inline PING
		c.sendInline("PING")
		resp := c.readReply()
		if resp != "PONG" {
			t.Errorf("expected PONG for inline, got %q", resp)
		}

		// Inline SET
		c.sendInline("SET foo bar")
		resp = c.readReply()
		if resp != "OK" {
			t.Errorf("expected OK for inline SET, got %q", resp)
		}

		// Inline GET
		c.sendInline("GET foo")
		resp = c.readReply()
		if resp != "bar" {
			t.Errorf("expected bar for inline GET, got %q", resp)
		}
	}
}

// =============================================================================
// String Commands Tests
// =============================================================================

func TestCorrectness_SetGet_Basic(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// SET and GET
		if resp := c.set("foo", "bar"); resp != "OK" {
			t.Errorf("expected OK, got %q", resp)
		}
		if val := c.get("foo"); val != "bar" {
			t.Errorf("expected bar, got %q", val)
		}

		// GET non-existent key
		if val := c.get("nonexistent"); val != "(nil)" {
			t.Errorf("expected (nil), got %q", val)
		}

		// Overwrite
		c.set("foo", "baz")
		if val := c.get("foo"); val != "baz" {
			t.Errorf("expected baz after overwrite, got %q", val)
		}
	}
}

func TestCorrectness_SetNX(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// SETNX on new key
		if resp := c.setNX("nxkey", "value1"); resp != "1" {
			t.Errorf("expected 1, got %q", resp)
		}

		// SETNX on existing key
		if resp := c.setNX("nxkey", "value2"); resp != "0" {
			t.Errorf("expected 0 for existing key, got %q", resp)
		}

		// Value should be unchanged
		if val := c.get("nxkey"); val != "value1" {
			t.Errorf("expected value1, got %q", val)
		}
	}
}

func TestCorrectness_SetWithNX(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// SET with NX on new key
		c.sendCommand("SET", "nxtest", "value1", "NX")
		if resp := c.readReply(); resp != "OK" {
			t.Errorf("expected OK, got %q", resp)
		}

		// SET with NX on existing key
		c.sendCommand("SET", "nxtest", "value2", "NX")
		if resp := c.readReply(); resp != "(nil)" {
			t.Errorf("expected (nil), got %q", resp)
		}
	}
}

func TestCorrectness_SetWithXX(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// SET with XX on non-existent key
		c.sendCommand("SET", "xxtest", "value1", "XX")
		if resp := c.readReply(); resp != "(nil)" {
			t.Errorf("expected (nil), got %q", resp)
		}

		// Create the key
		c.set("xxtest", "initial")

		// SET with XX on existing key
		c.sendCommand("SET", "xxtest", "updated", "XX")
		if resp := c.readReply(); resp != "OK" {
			t.Errorf("expected OK, got %q", resp)
		}

		if val := c.get("xxtest"); val != "updated" {
			t.Errorf("expected updated, got %q", val)
		}
	}
}

func TestCorrectness_SetEx(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// SET with EX
		if resp := c.setEx("exkey", 3600, "value"); resp != "OK" {
			t.Errorf("expected OK, got %q", resp)
		}

		// Check TTL
		ttl := c.ttl("exkey")
		if ttl <= 0 || ttl > 3600 {
			t.Errorf("expected TTL between 1 and 3600, got %d", ttl)
		}
	}
}

func TestCorrectness_SetPx(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// SET with PX
		if resp := c.setPx("pxkey", 60000, "value"); resp != "OK" {
			t.Errorf("expected OK, got %q", resp)
		}

		// Check PTTL
		pttl := c.pttl("pxkey")
		if pttl <= 0 || pttl > 60000 {
			t.Errorf("expected PTTL between 1 and 60000, got %d", pttl)
		}
	}
}

func TestCorrectness_GetSet(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// GETSET on new key
		c.sendCommand("GETSET", "gskey", "value1")
		if resp := c.readReply(); resp != "(nil)" {
			t.Errorf("expected (nil) for new key, got %q", resp)
		}

		// GETSET on existing key
		c.sendCommand("GETSET", "gskey", "value2")
		if resp := c.readReply(); resp != "value1" {
			t.Errorf("expected value1, got %q", resp)
		}

		// Verify new value
		if val := c.get("gskey"); val != "value2" {
			t.Errorf("expected value2, got %q", val)
		}
	}
}

func TestCorrectness_GetDel(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("gdkey", "value")

		// GETDEL
		c.sendCommand("GETDEL", "gdkey")
		if resp := c.readReply(); resp != "value" {
			t.Errorf("expected value, got %q", resp)
		}

		// Key should be gone
		if val := c.get("gdkey"); val != "(nil)" {
			t.Errorf("key should be deleted, got %q", val)
		}

		// GETDEL on non-existent
		c.sendCommand("GETDEL", "nonexistent")
		if resp := c.readReply(); resp != "(nil)" {
			t.Errorf("expected (nil), got %q", resp)
		}
	}
}

func TestCorrectness_Del(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("del1", "v1")
		c.set("del2", "v2")
		c.set("del3", "v3")

		// DEL multiple keys
		count := c.del("del1", "del2", "nonexistent")
		if count != 2 {
			t.Errorf("expected 2 deleted, got %d", count)
		}

		// Verify deletion
		if c.exists("del1") != 0 {
			t.Error("del1 should not exist")
		}
		if c.exists("del3") != 1 {
			t.Error("del3 should still exist")
		}
	}
}

func TestCorrectness_Exists(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("e1", "v1")
		c.set("e2", "v2")

		// EXISTS single
		if c.exists("e1") != 1 {
			t.Error("e1 should exist")
		}
		if c.exists("nonexistent") != 0 {
			t.Error("nonexistent should not exist")
		}

		// EXISTS multiple (counts occurrences)
		if c.exists("e1", "e2", "e1", "nonexistent") != 3 {
			t.Error("expected 3 (e1 counted twice, e2 once)")
		}
	}
}

func TestCorrectness_Type(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("strkey", "value")
		c.lpush("listkey", "item")
		c.hset("hashkey", "field", "value")

		if typ := c.typeCmd("strkey"); typ != "string" {
			t.Errorf("expected string, got %q", typ)
		}
		if typ := c.typeCmd("listkey"); typ != "list" {
			t.Errorf("expected list, got %q", typ)
		}
		if typ := c.typeCmd("hashkey"); typ != "hash" {
			t.Errorf("expected hash, got %q", typ)
		}
		if typ := c.typeCmd("nonexistent"); typ != "none" {
			t.Errorf("expected none, got %q", typ)
		}
	}
}

func TestCorrectness_IncrDecr(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("num", "10")

		// INCR
		if resp := c.incr("num"); resp != "11" {
			t.Errorf("expected 11, got %q", resp)
		}

		// DECR
		if resp := c.decr("num"); resp != "10" {
			t.Errorf("expected 10, got %q", resp)
		}

		// INCRBY
		if resp := c.incrBy("num", 5); resp != "15" {
			t.Errorf("expected 15, got %q", resp)
		}

		// DECRBY
		if resp := c.decrBy("num", 3); resp != "12" {
			t.Errorf("expected 12, got %q", resp)
		}

		// INCR on non-existent (starts at 0)
		if resp := c.incr("newnum"); resp != "1" {
			t.Errorf("expected 1, got %q", resp)
		}
	}
}

func TestCorrectness_IncrNegative(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("negnum", "0")

		// DECR below zero
		if resp := c.decr("negnum"); resp != "-1" {
			t.Errorf("expected -1, got %q", resp)
		}

		// INCRBY negative
		if resp := c.incrBy("negnum", -5); resp != "-6" {
			t.Errorf("expected -6, got %q", resp)
		}
	}
}

func TestCorrectness_IncrNonNumeric(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("text", "hello")

		resp := c.incr("text")
		if !strings.HasPrefix(resp, "-ERR") {
			t.Errorf("expected error for non-numeric incr, got %q", resp)
		}
	}
}

func TestCorrectness_IncrByFloat(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("floatnum", "10.5")

		c.sendCommand("INCRBYFLOAT", "floatnum", "0.1")
		resp := c.readReply()
		// Result should be around 10.6
		if !strings.HasPrefix(resp, "10.6") {
			t.Errorf("expected ~10.6, got %q", resp)
		}
	}
}

func TestCorrectness_Append(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("appkey", "Hello")

		// APPEND
		length := c.appendCmd("appkey", " World")
		if length != 11 {
			t.Errorf("expected length 11, got %d", length)
		}

		if val := c.get("appkey"); val != "Hello World" {
			t.Errorf("expected 'Hello World', got %q", val)
		}

		// APPEND to non-existent (creates key)
		length = c.appendCmd("newappkey", "value")
		if length != 5 {
			t.Errorf("expected length 5, got %d", length)
		}
	}
}

func TestCorrectness_Strlen(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("strlenkey", "Hello World")

		if length := c.strlen("strlenkey"); length != 11 {
			t.Errorf("expected 11, got %d", length)
		}

		// Non-existent key
		if length := c.strlen("nonexistent"); length != 0 {
			t.Errorf("expected 0, got %d", length)
		}
	}
}

func TestCorrectness_Mget(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("m1", "v1")
		c.set("m2", "v2")
		c.set("m3", "v3")

		resp := c.mget("m1", "m2", "nonexistent", "m3")
		// Should be comma-joined: v1,v2,(nil),v3
		if resp != "v1,v2,(nil),v3" {
			t.Errorf("expected 'v1,v2,(nil),v3', got %q", resp)
		}
	}
}

func TestCorrectness_Mset(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		if resp := c.mset("ms1", "val1", "ms2", "val2", "ms3", "val3"); resp != "OK" {
			t.Errorf("expected OK, got %q", resp)
		}

		if c.get("ms1") != "val1" || c.get("ms2") != "val2" || c.get("ms3") != "val3" {
			t.Error("MSET values not correctly set")
		}
	}
}

func TestCorrectness_GetRange(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("rangekey", "Hello World")

		c.sendCommand("GETRANGE", "rangekey", "0", "4")
		if resp := c.readReply(); resp != "Hello" {
			t.Errorf("expected Hello, got %q", resp)
		}

		c.sendCommand("GETRANGE", "rangekey", "-5", "-1")
		if resp := c.readReply(); resp != "World" {
			t.Errorf("expected World, got %q", resp)
		}
	}
}

func TestCorrectness_SetRange(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("srkey", "Hello World")

		c.sendCommand("SETRANGE", "srkey", "6", "Redis")
		resp := c.readReply()
		if resp != "11" {
			t.Errorf("expected 11, got %q", resp)
		}

		if val := c.get("srkey"); val != "Hello Redis" {
			t.Errorf("expected 'Hello Redis', got %q", val)
		}
	}
}

func TestCorrectness_Substr(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("mykey", "Hello World")

		// Positive indices
		c.sendCommand("SUBSTR", "mykey", "0", "4")
		if resp := c.readReply(); resp != "Hello" {
			t.Errorf("expected Hello, got %q", resp)
		}

		// Negative indices
		c.sendCommand("SUBSTR", "mykey", "-5", "-1")
		if resp := c.readReply(); resp != "World" {
			t.Errorf("expected World, got %q", resp)
		}

		// Non-existent key
		c.sendCommand("SUBSTR", "nonexistent", "0", "5")
		if resp := c.readReply(); resp != "" {
			t.Errorf("expected empty string, got %q", resp)
		}
	}
}

func TestCorrectness_Lcs(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Setup
		c.set("key1", "ohmytext")
		c.set("key2", "mynewtext")

		// Basic LCS
		c.sendCommand("LCS", "key1", "key2")
		if resp := c.readReply(); resp != "mytext" {
			t.Errorf("expected mytext, got %q", resp)
		}

		// LCS LEN
		c.sendCommand("LCS", "key1", "key2", "LEN")
		if resp := c.readIntReply(); resp != 6 {
			t.Errorf("expected 6, got %d", resp)
		}

		// Non-existent keys
		c.sendCommand("LCS", "nonexistent1", "nonexistent2")
		if resp := c.readReply(); resp != "" {
			t.Errorf("expected empty, got %q", resp)
		}

		c.sendCommand("LCS", "nonexistent1", "nonexistent2", "LEN")
		if resp := c.readIntReply(); resp != 0 {
			t.Errorf("expected 0, got %d", resp)
		}

		// One key exists, one doesn't
		c.sendCommand("LCS", "key1", "nonexistent", "LEN")
		if resp := c.readIntReply(); resp != 0 {
			t.Errorf("expected 0, got %d", resp)
		}

		// Edge case: identical strings
		c.set("same1", "hello")
		c.set("same2", "hello")
		c.sendCommand("LCS", "same1", "same2")
		if resp := c.readReply(); resp != "hello" {
			t.Errorf("expected hello, got %q", resp)
		}

		// Edge case: no common subsequence
		c.set("abc", "abc")
		c.set("xyz", "xyz")
		c.sendCommand("LCS", "abc", "xyz")
		if resp := c.readReply(); resp != "" {
			t.Errorf("expected empty, got %q", resp)
		}
	}
}

func TestCorrectness_Lcs_Idx(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("key1", "ohmytext")
		c.set("key2", "mynewtext")

		// IDX mode - returns array with matches
		c.sendCommand("LCS", "key1", "key2", "IDX")
		resp := c.readReply()
		// Should be an array starting with "matches"
		if resp == "" {
			t.Error("expected non-empty IDX response")
		}

		// IDX with MINMATCHLEN - filter out short matches
		c.sendCommand("LCS", "key1", "key2", "IDX", "MINMATCHLEN", "4")
		resp = c.readReply()
		if resp == "" {
			t.Error("expected non-empty IDX response with MINMATCHLEN")
		}

		// IDX with WITHMATCHLEN
		c.sendCommand("LCS", "key1", "key2", "IDX", "WITHMATCHLEN")
		resp = c.readReply()
		if resp == "" {
			t.Error("expected non-empty IDX response with WITHMATCHLEN")
		}

		// Test MINMATCHLEN that filters all matches
		c.sendCommand("LCS", "key1", "key2", "IDX", "MINMATCHLEN", "100")
		resp = c.readReply()
		// Should still have the structure, just empty matches
		if resp == "" {
			t.Error("expected non-empty IDX response even with high MINMATCHLEN")
		}
	}
}

// =============================================================================
// Expiration Tests
// =============================================================================

func TestCorrectness_Expire(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("expkey", "value")

		// Set expiration
		if result := c.expire("expkey", 3600); result != 1 {
			t.Errorf("expected 1, got %d", result)
		}

		// Check TTL
		ttl := c.ttl("expkey")
		if ttl <= 0 || ttl > 3600 {
			t.Errorf("expected TTL between 1 and 3600, got %d", ttl)
		}

		// EXPIRE on non-existent
		if result := c.expire("nonexistent", 100); result != 0 {
			t.Errorf("expected 0 for non-existent, got %d", result)
		}
	}
}

func TestCorrectness_TTL(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Non-existent key
		if ttl := c.ttl("nonexistent"); ttl != -2 {
			t.Errorf("expected -2 for non-existent, got %d", ttl)
		}

		// Key without TTL
		c.set("noxpkey", "value")
		if ttl := c.ttl("noxpkey"); ttl != -1 {
			t.Errorf("expected -1 for no expiry, got %d", ttl)
		}

		// Key with TTL
		c.expire("noxpkey", 100)
		ttl := c.ttl("noxpkey")
		if ttl <= 0 || ttl > 100 {
			t.Errorf("expected TTL between 1 and 100, got %d", ttl)
		}
	}
}

func TestCorrectness_Persist(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("persistkey", "value")
		c.expire("persistkey", 100)

		// Remove expiration
		if result := c.persist("persistkey"); result != 1 {
			t.Errorf("expected 1, got %d", result)
		}

		// TTL should now be -1
		if ttl := c.ttl("persistkey"); ttl != -1 {
			t.Errorf("expected -1 after persist, got %d", ttl)
		}

		// PERSIST on key without TTL
		if result := c.persist("persistkey"); result != 0 {
			t.Errorf("expected 0 for key without TTL, got %d", result)
		}
	}
}

func TestCorrectness_ExpireAt(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("expat", "value")

		// Set to expire 1 hour from now
		futureTime := time.Now().Unix() + 3600
		c.sendCommand("EXPIREAT", "expat", strconv.FormatInt(futureTime, 10))
		if resp := c.readReply(); resp != "1" {
			t.Errorf("expected 1, got %q", resp)
		}

		ttl := c.ttl("expat")
		if ttl < 3590 || ttl > 3600 {
			t.Errorf("expected TTL around 3600, got %d", ttl)
		}
	}
}

// TestCorrectness_FiveKeysInFiveKeysOut mirrors the Redis test "5 keys in, 5 keys out"
// This test verifies that KEYS returns all keys including those with TTL
func TestCorrectness_FiveKeysInFiveKeysOut(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Test on both db0 and db9 (like Redis correctness tests)
		for _, dbNum := range []int{0, 9} {
			// Switch to database
			c.selectDB(dbNum)

			// Flush to start clean
			c.flushdb()

			// Create 5 keys: a (with TTL), t, e, s, foo (no TTL)
			c.set("a", "c")
			c.expire("a", 5)
			c.set("t", "c")
			c.set("e", "c")
			c.set("s", "c")
			c.set("foo", "b")

			// Get all keys
			keysResult := c.keys("*")

			// Parse the comma-separated result and sort
			keysList := strings.Split(keysResult, ",")
			if len(keysList) != 5 {
				t.Errorf("db%d: expected 5 keys, got %d: %q", dbNum, len(keysList), keysResult)
				continue
			}

			// Check all expected keys are present (without db prefix)
			expectedKeys := map[string]bool{"a": true, "e": true, "foo": true, "s": true, "t": true}
			for _, k := range keysList {
				if !expectedKeys[k] {
					t.Errorf("db%d: unexpected key: %q (keys: %q)", dbNum, k, keysResult)
				}
				delete(expectedKeys, k)
			}
			if len(expectedKeys) > 0 {
				t.Errorf("db%d: missing keys: %v", dbNum, expectedKeys)
			}

			// Cleanup
			c.del("a")
		}
	}
}

// TestCorrectness_ActiveExpireKeysIncrementally mirrors the Redis test
// "Redis should actively expire keys incrementally"
// This test verifies that keys with short TTL are actively expired in the background

// =============================================================================
// List Commands Tests
// =============================================================================

func TestCorrectness_LPushRPush(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// RPUSH
		if length := c.rpush("mylist", "a", "b", "c"); length != 3 {
			t.Errorf("expected 3, got %d", length)
		}

		// LPUSH
		if length := c.lpush("mylist", "x", "y", "z"); length != 6 {
			t.Errorf("expected 6, got %d", length)
		}

		// Check order: z, y, x, a, b, c
		if val := c.lindex("mylist", 0); val != "z" {
			t.Errorf("expected z at index 0, got %q", val)
		}
		if val := c.lindex("mylist", 3); val != "a" {
			t.Errorf("expected a at index 3, got %q", val)
		}
	}
}

func TestCorrectness_LPopRPop(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.rpush("poplist", "a", "b", "c", "d")

		// LPOP
		if val := c.lpop("poplist"); val != "a" {
			t.Errorf("expected a, got %q", val)
		}

		// RPOP
		if val := c.rpop("poplist"); val != "d" {
			t.Errorf("expected d, got %q", val)
		}

		// Check remaining: b, c
		if length := c.llen("poplist"); length != 2 {
			t.Errorf("expected 2, got %d", length)
		}

		// Pop from empty list
		c.lpop("poplist")
		c.lpop("poplist")
		if val := c.lpop("poplist"); val != "(nil)" {
			t.Errorf("expected (nil) from empty list, got %q", val)
		}
	}
}

func TestCorrectness_LLen(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Non-existent list
		if length := c.llen("nolist"); length != 0 {
			t.Errorf("expected 0, got %d", length)
		}

		c.rpush("lenlist", "a", "b", "c")
		if length := c.llen("lenlist"); length != 3 {
			t.Errorf("expected 3, got %d", length)
		}
	}
}

func TestCorrectness_LRange(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.rpush("rangelist", "a", "b", "c", "d", "e")

		// Full range
		if resp := c.lrange("rangelist", 0, -1); resp != "a,b,c,d,e" {
			t.Errorf("expected 'a,b,c,d,e', got %q", resp)
		}

		// Partial range
		if resp := c.lrange("rangelist", 1, 3); resp != "b,c,d" {
			t.Errorf("expected 'b,c,d', got %q", resp)
		}

		// Negative indices
		if resp := c.lrange("rangelist", -2, -1); resp != "d,e" {
			t.Errorf("expected 'd,e', got %q", resp)
		}

		// Out of range
		if resp := c.lrange("rangelist", 10, 20); resp != "" {
			t.Errorf("expected empty, got %q", resp)
		}
	}
}

func TestCorrectness_LIndex(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.rpush("idxlist", "a", "b", "c")

		if val := c.lindex("idxlist", 0); val != "a" {
			t.Errorf("expected a, got %q", val)
		}
		if val := c.lindex("idxlist", -1); val != "c" {
			t.Errorf("expected c, got %q", val)
		}
		if val := c.lindex("idxlist", 10); val != "(nil)" {
			t.Errorf("expected (nil), got %q", val)
		}
	}
}

func TestCorrectness_LSet(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.rpush("setlist", "a", "b", "c")

		c.sendCommand("LSET", "setlist", "1", "B")
		if resp := c.readReply(); resp != "OK" {
			t.Errorf("expected OK, got %q", resp)
		}

		if val := c.lindex("setlist", 1); val != "B" {
			t.Errorf("expected B, got %q", val)
		}

		// Out of range
		c.sendCommand("LSET", "setlist", "10", "X")
		resp := c.readReply()
		if !strings.HasPrefix(resp, "-ERR") {
			t.Errorf("expected error for out of range, got %q", resp)
		}
	}
}

func TestCorrectness_LTrim(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.rpush("trimlist", "a", "b", "c", "d", "e")

		c.sendCommand("LTRIM", "trimlist", "1", "3")
		if resp := c.readReply(); resp != "OK" {
			t.Errorf("expected OK, got %q", resp)
		}

		if resp := c.lrange("trimlist", 0, -1); resp != "b,c,d" {
			t.Errorf("expected 'b,c,d', got %q", resp)
		}
	}
}

func TestCorrectness_LRem(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.rpush("remlist", "a", "b", "a", "c", "a", "d")

		// Remove first 2 occurrences of "a"
		c.sendCommand("LREM", "remlist", "2", "a")
		if resp := c.readReply(); resp != "2" {
			t.Errorf("expected 2, got %q", resp)
		}

		// Should have: b, c, a, d
		if length := c.llen("remlist"); length != 4 {
			t.Errorf("expected 4, got %d", length)
		}
	}
}

func TestCorrectness_LPushX_RPushX(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// LPUSHX on non-existent list
		c.sendCommand("LPUSHX", "nolist", "a")
		if resp := c.readReply(); resp != "0" {
			t.Errorf("expected 0, got %q", resp)
		}

		// Create list
		c.rpush("pushxlist", "a")

		// LPUSHX on existing list
		c.sendCommand("LPUSHX", "pushxlist", "b")
		if resp := c.readReply(); resp != "2" {
			t.Errorf("expected 2, got %q", resp)
		}

		// RPUSHX on existing list
		c.sendCommand("RPUSHX", "pushxlist", "c")
		if resp := c.readReply(); resp != "3" {
			t.Errorf("expected 3, got %q", resp)
		}
	}
}

// =============================================================================
// Hash Commands Tests
// =============================================================================

func TestCorrectness_HSetHGet(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// HSET new field
		if count := c.hset("myhash", "field1", "value1"); count != 1 {
			t.Errorf("expected 1, got %d", count)
		}

		// HSET existing field
		if count := c.hset("myhash", "field1", "value2"); count != 0 {
			t.Errorf("expected 0 for existing field, got %d", count)
		}

		// HGET
		if val := c.hget("myhash", "field1"); val != "value2" {
			t.Errorf("expected value2, got %q", val)
		}

		// HGET non-existent field
		if val := c.hget("myhash", "nofield"); val != "(nil)" {
			t.Errorf("expected (nil), got %q", val)
		}
	}
}

func TestCorrectness_HSetMultiple(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// HSET multiple fields
		count := c.hset("multihash", "f1", "v1", "f2", "v2", "f3", "v3")
		if count != 3 {
			t.Errorf("expected 3, got %d", count)
		}

		if c.hget("multihash", "f2") != "v2" {
			t.Error("field f2 not set correctly")
		}
	}
}

func TestCorrectness_HDel(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.hset("delhash", "f1", "v1", "f2", "v2", "f3", "v3")

		// Delete multiple fields
		count := c.hdel("delhash", "f1", "f3", "nofield")
		if count != 2 {
			t.Errorf("expected 2, got %d", count)
		}

		// Check remaining
		if c.hexists("delhash", "f1") != 0 {
			t.Error("f1 should be deleted")
		}
		if c.hexists("delhash", "f2") != 1 {
			t.Error("f2 should exist")
		}
	}
}

func TestCorrectness_HExists(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.hset("exhash", "field", "value")

		if c.hexists("exhash", "field") != 1 {
			t.Error("field should exist")
		}
		if c.hexists("exhash", "nofield") != 0 {
			t.Error("nofield should not exist")
		}
		if c.hexists("nohash", "field") != 0 {
			t.Error("nohash should not exist")
		}
	}
}

func TestCorrectness_HLen(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		if c.hlen("nohash") != 0 {
			t.Error("expected 0 for non-existent hash")
		}

		c.hset("lenhash", "f1", "v1", "f2", "v2", "f3", "v3")
		if c.hlen("lenhash") != 3 {
			t.Error("expected 3 fields")
		}
	}
}

func TestCorrectness_HGetAll(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.hset("allhash", "name", "Alice", "age", "30")

		resp := c.hgetall("allhash")
		// Response should contain all field-value pairs
		if !strings.Contains(resp, "name") || !strings.Contains(resp, "Alice") ||
			!strings.Contains(resp, "age") || !strings.Contains(resp, "30") {
			t.Errorf("HGETALL missing data: %q", resp)
		}
	}
}

func TestCorrectness_HKeys(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.hset("keyhash", "name", "Alice", "age", "30", "city", "NYC")

		resp := c.hkeys("keyhash")
		if !strings.Contains(resp, "name") || !strings.Contains(resp, "age") || !strings.Contains(resp, "city") {
			t.Errorf("HKEYS missing keys: %q", resp)
		}
	}
}

func TestCorrectness_HVals(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.hset("valhash", "name", "Alice", "age", "30")

		resp := c.hvals("valhash")
		if !strings.Contains(resp, "Alice") || !strings.Contains(resp, "30") {
			t.Errorf("HVALS missing values: %q", resp)
		}
	}
}

func TestCorrectness_HMGet(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.hset("mgethash", "f1", "v1", "f2", "v2", "f3", "v3")

		c.sendCommand("HMGET", "mgethash", "f1", "nofield", "f3")
		resp := c.readReply()
		if resp != "v1,(nil),v3" {
			t.Errorf("expected 'v1,(nil),v3', got %q", resp)
		}
	}
}

func TestCorrectness_HIncrBy(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.hset("incrhash", "counter", "10")

		if resp := c.hincrby("incrhash", "counter", 5); resp != "15" {
			t.Errorf("expected 15, got %q", resp)
		}

		if resp := c.hincrby("incrhash", "counter", -3); resp != "12" {
			t.Errorf("expected 12, got %q", resp)
		}

		// New field starts at 0
		if resp := c.hincrby("incrhash", "newfield", 1); resp != "1" {
			t.Errorf("expected 1, got %q", resp)
		}
	}
}

func TestCorrectness_HSetNX(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// New field
		c.sendCommand("HSETNX", "nxhash", "field", "value1")
		if resp := c.readReply(); resp != "1" {
			t.Errorf("expected 1, got %q", resp)
		}

		// Existing field
		c.sendCommand("HSETNX", "nxhash", "field", "value2")
		if resp := c.readReply(); resp != "0" {
			t.Errorf("expected 0, got %q", resp)
		}

		// Value should be unchanged
		if val := c.hget("nxhash", "field"); val != "value1" {
			t.Errorf("expected value1, got %q", val)
		}
	}
}

// =============================================================================
// Set Commands Tests
// =============================================================================

func TestCorrectness_SAddSCard(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// SADD new members
		c.sendCommand("SADD", "myset", "a", "b", "c")
		if resp := c.readReply(); resp != "3" {
			t.Errorf("expected 3 added, got %q", resp)
		}

		// SADD duplicate members
		c.sendCommand("SADD", "myset", "a", "d")
		if resp := c.readReply(); resp != "1" {
			t.Errorf("expected 1 added (d only), got %q", resp)
		}

		// SCARD
		c.sendCommand("SCARD", "myset")
		if resp := c.readReply(); resp != "4" {
			t.Errorf("expected 4 members, got %q", resp)
		}

		// SCARD on non-existent key
		c.sendCommand("SCARD", "noset")
		if resp := c.readReply(); resp != "0" {
			t.Errorf("expected 0 for non-existent set, got %q", resp)
		}
	}
}

func TestCorrectness_SIsMember(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.sendCommand("SADD", "myset", "a", "b", "c")
		c.readReply()

		// Check existing member
		c.sendCommand("SISMEMBER", "myset", "a")
		if resp := c.readReply(); resp != "1" {
			t.Errorf("expected 1 for existing member, got %q", resp)
		}

		// Check non-existing member
		c.sendCommand("SISMEMBER", "myset", "z")
		if resp := c.readReply(); resp != "0" {
			t.Errorf("expected 0 for non-existing member, got %q", resp)
		}

		// Check on non-existent set
		c.sendCommand("SISMEMBER", "noset", "a")
		if resp := c.readReply(); resp != "0" {
			t.Errorf("expected 0 for non-existent set, got %q", resp)
		}
	}
}

func TestCorrectness_SMembers(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.sendCommand("SADD", "myset", "c", "a", "b")
		c.readReply()

		// SMEMBERS returns all members (order may vary)
		c.sendCommand("SMEMBERS", "myset")
		resp := c.readReply()
		if sortCSV(resp) != "a,b,c" {
			t.Errorf("expected a,b,c, got %q", resp)
		}

		// SMEMBERS on non-existent set returns empty array (empty string after join)
		c.sendCommand("SMEMBERS", "noset")
		if resp := c.readReply(); resp != "" {
			t.Errorf("expected empty string for empty set, got %q", resp)
		}
	}
}

func TestCorrectness_SRem(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.sendCommand("SADD", "myset", "a", "b", "c", "d")
		c.readReply()

		// Remove existing members
		c.sendCommand("SREM", "myset", "a", "c")
		if resp := c.readReply(); resp != "2" {
			t.Errorf("expected 2 removed, got %q", resp)
		}

		// Verify removed
		c.sendCommand("SISMEMBER", "myset", "a")
		if resp := c.readReply(); resp != "0" {
			t.Errorf("expected 0, a should be removed, got %q", resp)
		}

		// Remove non-existing member
		c.sendCommand("SREM", "myset", "z")
		if resp := c.readReply(); resp != "0" {
			t.Errorf("expected 0, z not in set, got %q", resp)
		}

		// Remove from non-existent set
		c.sendCommand("SREM", "noset", "a")
		if resp := c.readReply(); resp != "0" {
			t.Errorf("expected 0 for non-existent set, got %q", resp)
		}
	}
}

func TestCorrectness_SPop(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.sendCommand("SADD", "myset", "a", "b", "c")
		c.readReply()

		// SPOP returns a member and removes it
		c.sendCommand("SPOP", "myset")
		resp := c.readReply()
		if resp != "a" && resp != "b" && resp != "c" {
			t.Errorf("expected a, b, or c, got %q", resp)
		}

		// Set should now have 2 members
		c.sendCommand("SCARD", "myset")
		if resp := c.readReply(); resp != "2" {
			t.Errorf("expected 2 after pop, got %q", resp)
		}

		// SPOP from empty set
		c.sendCommand("SPOP", "noset")
		if resp := c.readReply(); resp != "(nil)" {
			t.Errorf("expected (nil), got %q", resp)
		}
	}
}

func TestCorrectness_SetWrongType(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Create a string
		c.set("mystring", "value")

		// Try set operation on string
		c.sendCommand("SADD", "mystring", "member")
		resp := c.readReply()
		if !strings.Contains(resp, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE error, got %q", resp)
		}
	}
}

func TestCorrectness_SInter(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Create two sets with some overlap
		c.sendCommand("SADD", "set1", "a", "b", "c", "d")
		c.readReply()
		c.sendCommand("SADD", "set2", "c", "d", "e", "f")
		c.readReply()

		// SINTER returns intersection
		c.sendCommand("SINTER", "set1", "set2")
		resp := c.readReply()
		if sortCSV(resp) != "c,d" {
			t.Errorf("expected c,d, got %q", resp)
		}

		// SINTER with non-existent set returns empty
		c.sendCommand("SINTER", "set1", "noset")
		resp = c.readReply()
		if resp != "" {
			t.Errorf("expected empty for intersection with non-existent set, got %q", resp)
		}

		// SINTER with single set returns all members
		c.sendCommand("SINTER", "set1")
		resp = c.readReply()
		if sortCSV(resp) != "a,b,c,d" {
			t.Errorf("expected a,b,c,d, got %q", resp)
		}

		// SINTER with three sets
		c.sendCommand("SADD", "set3", "c", "g", "h")
		c.readReply()
		c.sendCommand("SINTER", "set1", "set2", "set3")
		resp = c.readReply()
		if resp != "c" {
			t.Errorf("expected c, got %q", resp)
		}
	}
}

func TestCorrectness_SInterStore(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Create two sets
		c.sendCommand("SADD", "set1", "a", "b", "c", "d")
		c.readReply()
		c.sendCommand("SADD", "set2", "c", "d", "e", "f")
		c.readReply()

		// SINTERSTORE stores intersection and returns cardinality
		c.sendCommand("SINTERSTORE", "dest", "set1", "set2")
		resp := c.readReply()
		if resp != "2" {
			t.Errorf("expected 2, got %q", resp)
		}

		// Verify destination set contents
		c.sendCommand("SMEMBERS", "dest")
		resp = c.readReply()
		if sortCSV(resp) != "c,d" {
			t.Errorf("expected c,d in dest, got %q", resp)
		}

		// SINTERSTORE with empty result deletes destination
		c.sendCommand("SINTERSTORE", "dest2", "set1", "noset")
		resp = c.readReply()
		if resp != "0" {
			t.Errorf("expected 0 for empty intersection, got %q", resp)
		}

		c.sendCommand("EXISTS", "dest2")
		resp = c.readReply()
		if resp != "0" {
			t.Errorf("dest2 should not exist after empty intersection, got %q", resp)
		}
	}
}

func TestCorrectness_SInterCard(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Create two sets
		c.sendCommand("SADD", "set1", "a", "b", "c", "d", "e")
		c.readReply()
		c.sendCommand("SADD", "set2", "c", "d", "e", "f", "g")
		c.readReply()

		// SINTERCARD returns cardinality of intersection
		c.sendCommand("SINTERCARD", "2", "set1", "set2")
		resp := c.readReply()
		if resp != "3" {
			t.Errorf("expected 3, got %q", resp)
		}

		// SINTERCARD with LIMIT
		c.sendCommand("SINTERCARD", "2", "set1", "set2", "LIMIT", "2")
		resp = c.readReply()
		if resp != "2" {
			t.Errorf("expected 2 with limit, got %q", resp)
		}

		// SINTERCARD with non-existent set
		c.sendCommand("SINTERCARD", "2", "set1", "noset")
		resp = c.readReply()
		if resp != "0" {
			t.Errorf("expected 0 for intersection with non-existent set, got %q", resp)
		}
	}
}

func TestCorrectness_SUnion(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Create two sets
		c.sendCommand("SADD", "set1", "a", "b", "c")
		c.readReply()
		c.sendCommand("SADD", "set2", "c", "d", "e")
		c.readReply()

		// SUNION returns union
		c.sendCommand("SUNION", "set1", "set2")
		resp := c.readReply()
		if sortCSV(resp) != "a,b,c,d,e" {
			t.Errorf("expected a,b,c,d,e, got %q", resp)
		}

		// SUNION with non-existent set returns existing set's members
		c.sendCommand("SUNION", "set1", "noset")
		resp = c.readReply()
		if sortCSV(resp) != "a,b,c" {
			t.Errorf("expected a,b,c, got %q", resp)
		}

		// SUNION with only non-existent sets returns empty
		c.sendCommand("SUNION", "noset1", "noset2")
		resp = c.readReply()
		if resp != "" {
			t.Errorf("expected empty for union of non-existent sets, got %q", resp)
		}
	}
}

func TestCorrectness_SUnionStore(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Create two sets
		c.sendCommand("SADD", "set1", "a", "b", "c")
		c.readReply()
		c.sendCommand("SADD", "set2", "c", "d", "e")
		c.readReply()

		// SUNIONSTORE stores union and returns cardinality
		c.sendCommand("SUNIONSTORE", "dest", "set1", "set2")
		resp := c.readReply()
		if resp != "5" {
			t.Errorf("expected 5, got %q", resp)
		}

		// Verify destination set contents
		c.sendCommand("SMEMBERS", "dest")
		resp = c.readReply()
		if sortCSV(resp) != "a,b,c,d,e" {
			t.Errorf("expected a,b,c,d,e in dest, got %q", resp)
		}

		// SUNIONSTORE with all non-existent sets
		c.sendCommand("SUNIONSTORE", "dest2", "noset1", "noset2")
		resp = c.readReply()
		if resp != "0" {
			t.Errorf("expected 0 for empty union, got %q", resp)
		}
	}
}

func TestCorrectness_SDiff(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Create sets
		c.sendCommand("SADD", "set1", "a", "b", "c", "d")
		c.readReply()
		c.sendCommand("SADD", "set2", "c", "d", "e")
		c.readReply()

		// SDIFF returns elements in first set but not in others
		c.sendCommand("SDIFF", "set1", "set2")
		resp := c.readReply()
		if sortCSV(resp) != "a,b" {
			t.Errorf("expected a,b, got %q", resp)
		}

		// SDIFF with multiple sets
		c.sendCommand("SADD", "set3", "a")
		c.readReply()
		c.sendCommand("SDIFF", "set1", "set2", "set3")
		resp = c.readReply()
		if resp != "b" {
			t.Errorf("expected b, got %q", resp)
		}

		// SDIFF with non-existent first set returns empty
		c.sendCommand("SDIFF", "noset", "set1")
		resp = c.readReply()
		if resp != "" {
			t.Errorf("expected empty for diff from non-existent set, got %q", resp)
		}

		// SDIFF with non-existent other sets returns first set
		c.sendCommand("SDIFF", "set1", "noset1", "noset2")
		resp = c.readReply()
		if sortCSV(resp) != "a,b,c,d" {
			t.Errorf("expected a,b,c,d, got %q", resp)
		}
	}
}

func TestCorrectness_SDiffStore(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Create sets
		c.sendCommand("SADD", "set1", "a", "b", "c", "d")
		c.readReply()
		c.sendCommand("SADD", "set2", "c", "d", "e")
		c.readReply()

		// SDIFFSTORE stores difference and returns cardinality
		c.sendCommand("SDIFFSTORE", "dest", "set1", "set2")
		resp := c.readReply()
		if resp != "2" {
			t.Errorf("expected 2, got %q", resp)
		}

		// Verify destination set contents
		c.sendCommand("SMEMBERS", "dest")
		resp = c.readReply()
		if sortCSV(resp) != "a,b" {
			t.Errorf("expected a,b in dest, got %q", resp)
		}

		// SDIFFSTORE with non-existent first set
		c.sendCommand("SDIFFSTORE", "dest2", "noset", "set1")
		resp = c.readReply()
		if resp != "0" {
			t.Errorf("expected 0 for diff from non-existent set, got %q", resp)
		}
	}
}

func TestCorrectness_SMove(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Create source set
		c.sendCommand("SADD", "src", "a", "b", "c")
		c.readReply()
		c.sendCommand("SADD", "dst", "x", "y")
		c.readReply()

		// SMOVE moves member from src to dst
		c.sendCommand("SMOVE", "src", "dst", "a")
		resp := c.readReply()
		if resp != "1" {
			t.Errorf("expected 1, got %q", resp)
		}

		// Verify a is no longer in src
		c.sendCommand("SISMEMBER", "src", "a")
		resp = c.readReply()
		if resp != "0" {
			t.Errorf("expected 0, a should be removed from src, got %q", resp)
		}

		// Verify a is now in dst
		c.sendCommand("SISMEMBER", "dst", "a")
		resp = c.readReply()
		if resp != "1" {
			t.Errorf("expected 1, a should be in dst, got %q", resp)
		}

		// SMOVE non-existent member returns 0
		c.sendCommand("SMOVE", "src", "dst", "z")
		resp = c.readReply()
		if resp != "0" {
			t.Errorf("expected 0 for non-existent member, got %q", resp)
		}

		// SMOVE from non-existent set returns 0
		c.sendCommand("SMOVE", "noset", "dst", "a")
		resp = c.readReply()
		if resp != "0" {
			t.Errorf("expected 0 for non-existent source set, got %q", resp)
		}

		// SMOVE to non-existent set creates it
		c.sendCommand("SMOVE", "src", "newdst", "b")
		resp = c.readReply()
		if resp != "1" {
			t.Errorf("expected 1, got %q", resp)
		}

		// Verify new set was created
		c.sendCommand("SISMEMBER", "newdst", "b")
		resp = c.readReply()
		if resp != "1" {
			t.Errorf("expected 1, b should be in newdst, got %q", resp)
		}
	}
}

func TestCorrectness_SMove_EmptySource(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Create source set with one member
		c.sendCommand("SADD", "src", "a")
		c.readReply()
		c.sendCommand("SADD", "dst", "x")
		c.readReply()

		// Move the only member
		c.sendCommand("SMOVE", "src", "dst", "a")
		resp := c.readReply()
		if resp != "1" {
			t.Errorf("expected 1, got %q", resp)
		}

		// Source set should be deleted (empty)
		c.sendCommand("EXISTS", "src")
		resp = c.readReply()
		if resp != "0" {
			t.Errorf("expected 0, empty source set should be deleted, got %q", resp)
		}
	}
}

func TestCorrectness_SetOperationsWrongType(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Create a string and a set
		c.set("mystring", "value")
		c.sendCommand("SADD", "myset", "a", "b", "c")
		c.readReply()

		// SINTER with wrong type
		c.sendCommand("SINTER", "myset", "mystring")
		resp := c.readReply()
		if !strings.Contains(resp, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE error, got %q", resp)
		}

		// SUNION with wrong type
		c.sendCommand("SUNION", "mystring", "myset")
		resp = c.readReply()
		if !strings.Contains(resp, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE error, got %q", resp)
		}

		// SDIFF with wrong type (second key is string)
		c.sendCommand("SDIFF", "myset", "mystring")
		resp = c.readReply()
		if !strings.Contains(resp, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE error for SDIFF with second key wrong type, got %q", resp)
		}

		// SDIFF with wrong type (first key is string)
		c.sendCommand("SDIFF", "mystring", "myset")
		resp = c.readReply()
		if !strings.Contains(resp, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE error for SDIFF with first key wrong type, got %q", resp)
		}

		// SMOVE from wrong type
		c.sendCommand("SMOVE", "mystring", "myset", "a")
		resp = c.readReply()
		if !strings.Contains(resp, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE error, got %q", resp)
		}

		// SMOVE to wrong type
		c.sendCommand("SMOVE", "myset", "mystring", "a")
		resp = c.readReply()
		if !strings.Contains(resp, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE error, got %q", resp)
		}
	}
}

// =============================================================================
// Server Commands Tests
// =============================================================================

func TestCorrectness_DBSize(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		if size := c.dbsize(); size != 0 {
			t.Errorf("expected 0, got %d", size)
		}

		c.set("key1", "value1")
		c.set("key2", "value2")

		if size := c.dbsize(); size != 2 {
			t.Errorf("expected 2, got %d", size)
		}
	}
}

func TestCorrectness_FlushDB(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("key1", "value1")
		c.set("key2", "value2")

		if resp := c.flushdb(); resp != "OK" {
			t.Errorf("expected OK, got %q", resp)
		}

		if size := c.dbsize(); size != 0 {
			t.Errorf("expected 0 after flush, got %d", size)
		}

		// Verify key doesn't exist
		if val := c.get("key1"); val != "(nil)" {
			t.Errorf("expected (nil), got %q", val)
		}
	}
}

func TestCorrectness_FlushAll(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Set keys in multiple DBs
		c.set("key0", "value0")
		c.selectDB(1)
		c.set("key1", "value1")

		if resp := c.flushall(); resp != "OK" {
			t.Errorf("expected OK, got %q", resp)
		}

		// Check DB 1
		if size := c.dbsize(); size != 0 {
			t.Errorf("expected 0 in DB 1, got %d", size)
		}

		// Check DB 0
		c.selectDB(0)
		if size := c.dbsize(); size != 0 {
			t.Errorf("expected 0 in DB 0, got %d", size)
		}
	}
}

func TestCorrectness_FlushDB_ThenReuse(t *testing.T) {
	addr, cleanup := startRedisTestServerWithEffects(t)
	defer cleanup()

	c := newRedisClient(t, addr)
	defer c.close()

	// Create a list with 3 elements
	c.sendCommand("LPUSH", "mylist", "a")
	c.readReply()
	c.sendCommand("LPUSH", "mylist", "b")
	c.readReply()
	c.sendCommand("LPUSH", "mylist", "c")
	c.readReply()

	// Also set a scalar key
	c.set("mykey", "before")

	// Verify list length
	c.sendCommand("LLEN", "mylist")
	if resp := c.readIntReply(); resp != 3 {
		t.Fatalf("expected 3 before flush, got %d", resp)
	}

	// Flush
	if resp := c.flushdb(); resp != "OK" {
		t.Fatalf("expected OK, got %q", resp)
	}

	// Verify keys are gone
	if val := c.get("mykey"); val != "(nil)" {
		t.Fatalf("expected (nil) for mykey after flush, got %q", val)
	}

	// Reuse the same key names
	c.sendCommand("LPUSH", "mylist", "x")
	c.readReply()
	c.sendCommand("LPUSH", "mylist", "y")
	c.readReply()
	c.set("mykey", "after")

	// List should have exactly 2, not contaminated by pre-flush state
	c.sendCommand("LLEN", "mylist")
	if resp := c.readIntReply(); resp != 2 {
		t.Fatalf("expected 2 after flush+reuse, got %d", resp)
	}

	// Scalar should have new value
	if val := c.get("mykey"); val != "after" {
		t.Fatalf("expected 'after', got %q", val)
	}
}

func TestCorrectness_Keys(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("foo", "1")
		c.set("foobar", "2")
		c.set("bar", "3")
		c.set("hello", "4")

		// All keys
		c.sendCommand("KEYS", "*")
		resp := c.readReply()
		if !strings.Contains(resp, "foo") || !strings.Contains(resp, "bar") {
			t.Errorf("KEYS * should return all keys: %q", resp)
		}

		// Pattern matching
		c.sendCommand("KEYS", "foo*")
		resp = c.readReply()
		if !strings.Contains(resp, "foo") || !strings.Contains(resp, "foobar") {
			t.Errorf("KEYS foo* should match foo and foobar: %q", resp)
		}
		if strings.Contains(resp, "hello") {
			t.Errorf("KEYS foo* should not match hello: %q", resp)
		}
	}
}

func TestCorrectness_Time(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.sendCommand("TIME")
		resp := c.readReply()

		// Should be two numbers (seconds, microseconds)
		parts := strings.Split(resp, ",")
		if len(parts) != 2 {
			t.Errorf("expected 2 parts, got %q", resp)
		}

		seconds, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			t.Errorf("invalid seconds: %v", err)
		}

		// Should be a recent timestamp
		now := time.Now().Unix()
		if seconds < now-10 || seconds > now+10 {
			t.Errorf("timestamp seems wrong: %d vs %d", seconds, now)
		}
	}
}

func TestCorrectness_Info(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.sendCommand("INFO")
		resp := c.readReply()

		// Should contain various sections
		if !strings.Contains(resp, "redis_version") {
			t.Errorf("INFO should contain redis_version: %q", resp)
		}
	}
}

func TestCorrectness_Command(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.sendCommand("COMMAND")
		resp := c.readReply()

		// Should return array of commands
		if resp == "(nil)" || resp == "" {
			t.Error("COMMAND should return command list")
		}
	}
}

// =============================================================================
// Error Handling Tests
// =============================================================================

func TestCorrectness_WrongNumberOfArgs(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// GET without key
		c.sendCommand("GET")
		resp := c.readReply()
		if !strings.HasPrefix(resp, "-ERR") {
			t.Errorf("expected error for GET without args, got %q", resp)
		}

		// SET with only key
		c.sendCommand("SET", "key")
		resp = c.readReply()
		if !strings.HasPrefix(resp, "-ERR") {
			t.Errorf("expected error for SET with only key, got %q", resp)
		}
	}
}

func TestCorrectness_WrongType(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Create a string
		c.set("strkey", "value")

		// Try list operation on string
		c.sendCommand("LPUSH", "strkey", "item")
		resp := c.readReply()
		if !strings.Contains(resp, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE error, got %q", resp)
		}

		// Create a list
		c.rpush("listkey", "item")

		// Try hash operation on list
		c.sendCommand("HGET", "listkey", "field")
		resp = c.readReply()
		if !strings.Contains(resp, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE error, got %q", resp)
		}
	}
}

func TestCorrectness_UnknownCommand(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.sendCommand("UNKNOWNCOMMAND", "arg1", "arg2")
		resp := c.readReply()
		if !strings.HasPrefix(resp, "-ERR") {
			t.Errorf("expected error for unknown command, got %q", resp)
		}
	}
}

// =============================================================================
// Concurrency Tests
// =============================================================================

func TestCorrectness_ConcurrentClients(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		const numClients = 10
		const opsPerClient = 100

		var wg sync.WaitGroup
		errors := make(chan error, numClients*opsPerClient)

		for i := range numClients {
			wg.Add(1)
			go func(clientID int) {
				defer wg.Done()

				c := newRedisClient(t, addr)
				defer c.close()

				for j := range opsPerClient {
					key := fmt.Sprintf("key_%d_%d", clientID, j)
					value := fmt.Sprintf("value_%d_%d", clientID, j)

					if resp := c.set(key, value); resp != "OK" {
						errors <- fmt.Errorf("client %d: set failed: %q", clientID, resp)
						continue
					}

					if val := c.get(key); val != value {
						errors <- fmt.Errorf("client %d: expected %q, got %q", clientID, value, val)
					}
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		for err := range errors {
			t.Error(err)
		}
	}
}

func TestCorrectness_ConcurrentIncr(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		c.set("counter", "0")
		c.close()

		const numClients = 10
		const incrsPerClient = 100

		var wg sync.WaitGroup

		for range numClients {
			wg.Go(func() {

				client := newRedisClient(t, addr)
				defer client.close()

				for range incrsPerClient {
					client.incr("counter")
				}
			})
		}

		wg.Wait()

		// Verify final count
		c2 := newRedisClient(t, addr)
		defer c2.close()

		expected := strconv.Itoa(numClients * incrsPerClient)
		if val := c2.get("counter"); val != expected {
			t.Errorf("expected %s, got %q", expected, val)
		}
	}
}

// =============================================================================
// Pipelining Tests
// =============================================================================

func TestCorrectness_Pipeline(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Send multiple commands without reading
		c.sendCommand("SET", "p1", "v1")
		c.sendCommand("SET", "p2", "v2")
		c.sendCommand("GET", "p1")
		c.sendCommand("GET", "p2")
		c.sendCommand("DEL", "p1", "p2")

		// Read all responses
		if resp := c.readReply(); resp != "OK" {
			t.Errorf("expected OK for SET p1, got %q", resp)
		}
		if resp := c.readReply(); resp != "OK" {
			t.Errorf("expected OK for SET p2, got %q", resp)
		}
		if resp := c.readReply(); resp != "v1" {
			t.Errorf("expected v1, got %q", resp)
		}
		if resp := c.readReply(); resp != "v2" {
			t.Errorf("expected v2, got %q", resp)
		}
		if resp := c.readReply(); resp != "2" {
			t.Errorf("expected 2 deleted, got %q", resp)
		}
	}
}

// =============================================================================
// Large Data Tests
// =============================================================================

func TestCorrectness_LargeValue(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// 100KB value
		value := strings.Repeat("X", 100*1024)
		if resp := c.set("largekey", value); resp != "OK" {
			t.Fatalf("failed to set large value: %q", resp)
		}

		if got := c.get("largekey"); got != value {
			t.Errorf("large value mismatch: lengths %d vs %d", len(got), len(value))
		}
	}
}

func TestCorrectness_ManyKeys(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		const numKeys = 1000

		// Set many keys
		for i := range numKeys {
			c.set(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		}

		if size := c.dbsize(); size != numKeys {
			t.Errorf("expected %d keys, got %d", numKeys, size)
		}

		// Verify a few random keys
		if val := c.get("key0"); val != "value0" {
			t.Errorf("expected value0, got %q", val)
		}
		if val := c.get("key999"); val != "value999" {
			t.Errorf("expected value999, got %q", val)
		}
	}
}

func TestCorrectness_LargeList(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		const numItems = 1000

		// Add many items
		for i := range numItems {
			c.rpush("largelist", fmt.Sprintf("item%d", i))
		}

		if length := c.llen("largelist"); length != numItems {
			t.Errorf("expected %d items, got %d", numItems, length)
		}

		// Verify first and last
		if val := c.lindex("largelist", 0); val != "item0" {
			t.Errorf("expected item0, got %q", val)
		}
		if val := c.lindex("largelist", -1); val != fmt.Sprintf("item%d", numItems-1) {
			t.Errorf("expected item%d, got %q", numItems-1, val)
		}
	}
}

func TestCorrectness_LargeHash(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		const numFields = 500

		// Add many fields
		for i := range numFields {
			c.hset("largehash", fmt.Sprintf("field%d", i), fmt.Sprintf("value%d", i))
		}

		if length := c.hlen("largehash"); length != numFields {
			t.Errorf("expected %d fields, got %d", numFields, length)
		}

		// Verify a few fields
		if val := c.hget("largehash", "field0"); val != "value0" {
			t.Errorf("expected value0, got %q", val)
		}
		if val := c.hget("largehash", fmt.Sprintf("field%d", numFields-1)); val != fmt.Sprintf("value%d", numFields-1) {
			t.Errorf("expected value%d, got %q", numFields-1, val)
		}
	}
}

// =============================================================================
// Binary Safety Tests
// =============================================================================

func TestCorrectness_BinaryValue(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Value with null bytes and special characters
		value := "hello\x00world\x01\x02\xff"
		c.set("binkey", value)

		if got := c.get("binkey"); got != value {
			t.Errorf("binary value mismatch: %q vs %q", got, value)
		}
	}
}

func TestCorrectness_EmptyValue(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("emptykey", "")
		if val := c.get("emptykey"); val != "" {
			t.Errorf("expected empty string, got %q", val)
		}

		// Empty value should still exist
		if c.exists("emptykey") != 1 {
			t.Error("empty value key should exist")
		}
	}
}

// =============================================================================
// Edge Cases
// =============================================================================

func TestCorrectness_KeysWithSpaces(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("key with spaces", "value with spaces")
		if val := c.get("key with spaces"); val != "value with spaces" {
			t.Errorf("expected 'value with spaces', got %q", val)
		}
	}
}

func TestCorrectness_SpecialCharKeys(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		keys := []string{
			"key:with:colons",
			"key.with.dots",
			"key-with-dashes",
			"key_with_underscores",
			"key{with}braces",
			"key[with]brackets",
		}

		for _, key := range keys {
			c.set(key, "value")
			if val := c.get(key); val != "value" {
				t.Errorf("failed for key %q: got %q", key, val)
			}
		}
	}
}

func TestCorrectness_NewItemsAfterFlush(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		c.set("foo", "old")
		c.flushdb()

		// Set new value after flush
		c.set("foo", "new")
		if val := c.get("foo"); val != "new" {
			t.Errorf("expected new value after flush, got %q", val)
		}
	}
}

// RESP3 Protocol Tests

func TestCorrectness_HelloDefault(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// HELLO with no args returns server info (as RESP2 flat array)
		c.sendCommand("HELLO")
		reply := c.readReply()
		// Default is RESP2, so we should get a flat array
		if !strings.Contains(reply, "server") || !strings.Contains(reply, "swytch") {
			t.Errorf("HELLO should return server info, got: %s", reply)
		}
	}
}

func TestCorrectness_HelloRESP2(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// HELLO 2 explicitly requests RESP2
		c.sendCommand("HELLO", "2")
		reply := c.readReply()
		// Should get RESP2 flat array format (key,val,key,val,...)
		if !strings.Contains(reply, "proto") || !strings.Contains(reply, "2") {
			t.Errorf("HELLO 2 should return proto=2, got: %s", reply)
		}
	}
}

func TestCorrectness_HelloRESP3(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// HELLO 3 switches to RESP3
		c.sendCommand("HELLO", "3")
		reply := c.readReply()
		// Should get RESP3 map format
		if !strings.HasPrefix(reply, "map{") {
			t.Errorf("HELLO 3 should return RESP3 map, got: %s", reply)
		}
		if !strings.Contains(reply, "proto=3") {
			t.Errorf("HELLO 3 should report proto=3, got: %s", reply)
		}
	}
}

func TestCorrectness_HelloInvalidVersion(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// HELLO 4 is not supported
		c.sendCommand("HELLO", "4")
		reply := c.readReply()
		if !strings.Contains(reply, "NOPROTO") {
			t.Errorf("HELLO 4 should return NOPROTO error, got: %s", reply)
		}
	}
}

func TestCorrectness_RESP3HGetAll(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Setup hash
		c.sendCommand("HSET", "myhash", "field1", "value1", "field2", "value2")
		c.readReply()

		// Test RESP2 HGETALL (flat array)
		c.sendCommand("HGETALL", "myhash")
		resp2Reply := c.readReply()
		// RESP2 returns flat array
		if strings.HasPrefix(resp2Reply, "map{") {
			t.Errorf("RESP2 HGETALL should not return map, got: %s", resp2Reply)
		}

		// Switch to RESP3
		c.sendCommand("HELLO", "3")
		c.readReply()

		// Test RESP3 HGETALL (map)
		c.sendCommand("HGETALL", "myhash")
		resp3Reply := c.readReply()
		if !strings.HasPrefix(resp3Reply, "map{") {
			t.Errorf("RESP3 HGETALL should return map, got: %s", resp3Reply)
		}
		// Should contain our fields
		if !strings.Contains(resp3Reply, "field1=value1") || !strings.Contains(resp3Reply, "field2=value2") {
			t.Errorf("RESP3 HGETALL missing fields, got: %s", resp3Reply)
		}
	}
}

func TestCorrectness_RESP3EmptyHGetAll(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Switch to RESP3
		c.sendCommand("HELLO", "3")
		c.readReply()

		// HGETALL on non-existent key
		c.sendCommand("HGETALL", "nonexistent")
		reply := c.readReply()
		// Should return empty map
		if reply != "map{}" {
			t.Errorf("RESP3 HGETALL on nonexistent should return empty map, got: %s", reply)
		}
	}
}

func TestCorrectness_HelloWithSetname(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// HELLO 3 SETNAME myclient
		c.sendCommand("HELLO", "3", "SETNAME", "myclient")
		reply := c.readReply()
		if !strings.HasPrefix(reply, "map{") {
			t.Errorf("HELLO 3 SETNAME should return RESP3 map, got: %s", reply)
		}
	}
}

func TestCorrectness_RESP3PersistsAcrossCommands(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Switch to RESP3
		c.sendCommand("HELLO", "3")
		c.readReply()

		// Create a hash
		c.sendCommand("HSET", "testhash", "a", "1", "b", "2")
		c.readReply()

		// HGETALL should still use RESP3
		c.sendCommand("HGETALL", "testhash")
		reply := c.readReply()
		if !strings.HasPrefix(reply, "map{") {
			t.Errorf("RESP3 should persist across commands, HGETALL returned: %s", reply)
		}

		// Switch back to RESP2
		c.sendCommand("HELLO", "2")
		c.readReply()

		// HGETALL should now use RESP2 format
		c.sendCommand("HGETALL", "testhash")
		reply = c.readReply()
		if strings.HasPrefix(reply, "map{") {
			t.Errorf("After HELLO 2, HGETALL should not return map, got: %s", reply)
		}
	}
}

// =============================================================================
// SWAPDB Tests
// =============================================================================

func TestCorrectness_SwapDB_Basic(t *testing.T) {
	t.Skip("SWAPDB is not supported with the effects engine")
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Set data in DB 0
		c.selectDB(0)
		c.set("key0", "value0")

		// Set data in DB 1
		c.selectDB(1)
		c.set("key1", "value1")

		// Verify initial state
		c.selectDB(0)
		if val := c.get("key0"); val != "value0" {
			t.Errorf("DB0 before swap: expected value0, got %q", val)
		}
		if val := c.get("key1"); val != "(nil)" {
			t.Errorf("DB0 before swap: key1 should not exist, got %q", val)
		}

		c.selectDB(1)
		if val := c.get("key1"); val != "value1" {
			t.Errorf("DB1 before swap: expected value1, got %q", val)
		}
		if val := c.get("key0"); val != "(nil)" {
			t.Errorf("DB1 before swap: key0 should not exist, got %q", val)
		}

		// Swap DB 0 and DB 1
		if resp := c.swapDB(0, 1); resp != "OK" {
			t.Errorf("SWAPDB: expected OK, got %q", resp)
		}

		// Verify swapped state - DB 0 now has DB 1's data
		c.selectDB(0)
		if val := c.get("key1"); val != "value1" {
			t.Errorf("DB0 after swap: expected value1, got %q", val)
		}
		if val := c.get("key0"); val != "(nil)" {
			t.Errorf("DB0 after swap: key0 should not exist, got %q", val)
		}

		// DB 1 now has DB 0's data
		c.selectDB(1)
		if val := c.get("key0"); val != "value0" {
			t.Errorf("DB1 after swap: expected value0, got %q", val)
		}
		if val := c.get("key1"); val != "(nil)" {
			t.Errorf("DB1 after swap: key1 should not exist, got %q", val)
		}
	}
}

func TestCorrectness_SwapDB_SameDB(t *testing.T) {
	t.Skip("SWAPDB is not supported with the effects engine")
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Set data in DB 0
		c.selectDB(0)
		c.set("mykey", "myvalue")

		// Swap DB 0 with itself (should be a no-op)
		if resp := c.swapDB(0, 0); resp != "OK" {
			t.Errorf("SWAPDB same db: expected OK, got %q", resp)
		}

		// Data should still be there
		if val := c.get("mykey"); val != "myvalue" {
			t.Errorf("after swap same db: expected myvalue, got %q", val)
		}
	}
}

func TestCorrectness_SwapDB_InvalidDB(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Try to swap with invalid DB index
		resp := c.swapDB(0, 999)
		if !strings.HasPrefix(resp, "-ERR") {
			t.Errorf("SWAPDB invalid db: expected error, got %q", resp)
		}

		// Try negative DB index
		c.sendCommand("SWAPDB", "-1", "0")
		resp = c.readReply()
		if !strings.HasPrefix(resp, "-ERR") {
			t.Errorf("SWAPDB negative db: expected error, got %q", resp)
		}
	}
}

func TestCorrectness_SwapDB_WrongArgs(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Too few args
		c.sendCommand("SWAPDB", "0")
		resp := c.readReply()
		if !strings.HasPrefix(resp, "-ERR") {
			t.Errorf("SWAPDB one arg: expected error, got %q", resp)
		}

		// Too many args
		c.sendCommand("SWAPDB", "0", "1", "2")
		resp = c.readReply()
		if !strings.HasPrefix(resp, "-ERR") {
			t.Errorf("SWAPDB three args: expected error, got %q", resp)
		}

		// Non-numeric args
		c.sendCommand("SWAPDB", "a", "b")
		resp = c.readReply()
		if !strings.HasPrefix(resp, "-ERR") {
			t.Errorf("SWAPDB non-numeric: expected error, got %q", resp)
		}
	}
}

func TestCorrectness_SwapDB_MultipleSwaps(t *testing.T) {
	t.Skip("SWAPDB is not supported with the effects engine")
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Set data in DB 0, 1, 2
		c.selectDB(0)
		c.set("k", "db0")
		c.selectDB(1)
		c.set("k", "db1")
		c.selectDB(2)
		c.set("k", "db2")

		// Swap 0 and 1
		c.swapDB(0, 1)

		// Now: DB0=db1, DB1=db0, DB2=db2
		c.selectDB(0)
		if val := c.get("k"); val != "db1" {
			t.Errorf("after first swap DB0: expected db1, got %q", val)
		}
		c.selectDB(1)
		if val := c.get("k"); val != "db0" {
			t.Errorf("after first swap DB1: expected db0, got %q", val)
		}

		// Swap 1 and 2
		c.swapDB(1, 2)

		// Now: DB0=db1, DB1=db2, DB2=db0
		c.selectDB(0)
		if val := c.get("k"); val != "db1" {
			t.Errorf("after second swap DB0: expected db1, got %q", val)
		}
		c.selectDB(1)
		if val := c.get("k"); val != "db2" {
			t.Errorf("after second swap DB1: expected db2, got %q", val)
		}
		c.selectDB(2)
		if val := c.get("k"); val != "db0" {
			t.Errorf("after second swap DB2: expected db0, got %q", val)
		}

		// Swap back 0 and 2
		c.swapDB(0, 2)

		// Now: DB0=db0, DB1=db2, DB2=db1
		c.selectDB(0)
		if val := c.get("k"); val != "db0" {
			t.Errorf("after third swap DB0: expected db0, got %q", val)
		}
	}
}

func TestCorrectness_SwapDB_WithDifferentTypes(t *testing.T) {
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Set string in DB 0
		c.selectDB(0)
		c.set("strkey", "stringvalue")

		// Set list in DB 1
		c.selectDB(1)
		c.sendCommand("LPUSH", "listkey", "item1", "item2")
		c.readReply()

		// Set hash in DB 2
		c.selectDB(2)
		c.sendCommand("HSET", "hashkey", "field", "value")
		c.readReply()

		// Swap 0 and 1
		c.swapDB(0, 1)

		// Verify types are correct after swap
		c.selectDB(0)
		if typ := c.typeCmd("listkey"); typ != "list" {
			t.Errorf("DB0 after swap: expected list type, got %q", typ)
		}

		c.selectDB(1)
		if typ := c.typeCmd("strkey"); typ != "string" {
			t.Errorf("DB1 after swap: expected string type, got %q", typ)
		}

		// Verify actual values
		c.selectDB(0)
		c.sendCommand("LRANGE", "listkey", "0", "-1")
		listResp := c.readReply()
		if !strings.Contains(listResp, "item1") || !strings.Contains(listResp, "item2") {
			t.Errorf("DB0 list content mismatch: %q", listResp)
		}

		c.selectDB(1)
		if val := c.get("strkey"); val != "stringvalue" {
			t.Errorf("DB1 string value mismatch: %q", val)
		}
	}
}

func TestCorrectness_SwapDB_EmptyDB(t *testing.T) {
	t.Skip("SWAPDB is not supported with the effects engine")
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c := newRedisClient(t, addr)
		defer c.close()

		// Set data only in DB 0
		c.selectDB(0)
		c.set("onlykey", "onlyvalue")

		// DB 1 is empty
		c.selectDB(1)
		if size := c.dbsize(); size != 0 {
			t.Errorf("DB1 should be empty, has %d keys", size)
		}

		// Swap
		c.swapDB(0, 1)

		// DB 0 should now be empty
		c.selectDB(0)
		if val := c.get("onlykey"); val != "(nil)" {
			t.Errorf("DB0 after swap should be empty, got %q", val)
		}

		// DB 1 should have the data
		c.selectDB(1)
		if val := c.get("onlykey"); val != "onlyvalue" {
			t.Errorf("DB1 after swap: expected onlyvalue, got %q", val)
		}
	}
}

func TestCorrectness_SwapDB_ConnectionPersistence(t *testing.T) {
	t.Skip("SWAPDB is not supported with the effects engine")
	{
		addr, cleanup := startRedisTestServer(t)
		defer cleanup()

		c1 := newRedisClient(t, addr)
		defer c1.close()
		c2 := newRedisClient(t, addr)
		defer c2.close()

		// c1 selects DB 0, c2 selects DB 1
		c1.selectDB(0)
		c2.selectDB(1)

		// Set data
		c1.set("key", "from_db0")
		c2.set("key", "from_db1")

		// Swap from c1's perspective
		c1.swapDB(0, 1)

		// c1 is still on DB 0 (by index), but DB 0 now contains what was DB 1's data
		if val := c1.get("key"); val != "from_db1" {
			t.Errorf("c1 on DB0 after swap: expected from_db1, got %q", val)
		}

		// c2 is still on DB 1 (by index), which now contains what was DB 0's data
		if val := c2.get("key"); val != "from_db0" {
			t.Errorf("c2 on DB1 after swap: expected from_db0, got %q", val)
		}
	}
}

// BLPOP inside transaction tests
func TestCorrectness_BLPOPInsideTransaction(t *testing.T) {
	addr, cleanup := startRedisTestServer(t)
	defer cleanup()

	c := newRedisClient(t, addr)
	defer c.close()

	// Switch to RESP3
	c.sendCommand("HELLO", "3")
	c.readReply()

	// Setup: create a list with two elements
	c.sendCommand("DEL", "xlist")
	c.readReply()

	c.sendCommand("LPUSH", "xlist", "foo")
	resp := c.readReply()
	if resp != "1" {
		t.Fatalf("LPUSH foo: expected 1, got %q", resp)
	}

	c.sendCommand("LPUSH", "xlist", "bar")
	resp = c.readReply()
	if resp != "2" {
		t.Fatalf("LPUSH bar: expected 2, got %q", resp)
	}

	// Start transaction
	c.sendCommand("MULTI")
	resp = c.readReply()
	if resp != "OK" {
		t.Fatalf("MULTI: expected OK, got %q", resp)
	}

	// Queue three BLPOP commands
	c.sendCommand("BLPOP", "xlist", "0")
	resp = c.readReply()
	if resp != "QUEUED" {
		t.Fatalf("BLPOP 1: expected QUEUED, got %q", resp)
	}

	c.sendCommand("BLPOP", "xlist", "0")
	resp = c.readReply()
	if resp != "QUEUED" {
		t.Fatalf("BLPOP 2: expected QUEUED, got %q", resp)
	}

	c.sendCommand("BLPOP", "xlist", "0")
	resp = c.readReply()
	if resp != "QUEUED" {
		t.Fatalf("BLPOP 3: expected QUEUED, got %q", resp)
	}

	// Execute transaction
	c.sendCommand("EXEC")
	resp = c.readReply()

	// Expected: [[xlist, bar], [xlist, foo], nil]
	// The list was [bar, foo] after LPUSHes, BLPOP pops from left
	// First BLPOP: returns [xlist, bar]
	// Second BLPOP: returns [xlist, foo]
	// Third BLPOP: returns nil (list empty)
	// The test client flattens nested arrays with commas
	expected := "xlist,bar,xlist,foo,(nil)"
	if resp != expected {
		t.Errorf("EXEC: expected %q, got %q", expected, resp)
	}
}

// TestCorrectness_BLPOP_NotificationsAfterFlush verifies that BLPOP notifications
// are deferred until effects are flushed (not fired mid-transaction).
// When LPUSH is the only data-modifying op in a MULTI/EXEC, BLPOP should
// receive data only AFTER the transaction commits.
func TestCorrectness_BLPOP_NotificationsAfterFlush(t *testing.T) {
	addr, cleanup := startRedisTestServerWithEffects(t)
	defer cleanup()

	c1 := newRedisClient(t, addr)
	defer c1.close()
	c2 := newRedisClient(t, addr)
	defer c2.close()

	// Clean slate
	c2.sendCommand("DEL", "list")
	c2.readReply()

	// Client 1: start BLPOP in a goroutine (it will block)
	var blpopResult string
	var blpopDone sync.WaitGroup
	blpopDone.Go(func() {
		c1.sendCommand("BLPOP", "list", "5")
		blpopResult = c1.readReply()
	})

	// Give BLPOP time to register
	time.Sleep(100 * time.Millisecond)

	// Client 2: MULTI { LPUSH list a } EXEC — notification fires after commit
	c2.sendCommand("MULTI")
	resp := c2.readReply()
	if resp != "OK" {
		t.Fatalf("MULTI: expected OK, got %q", resp)
	}

	c2.sendCommand("LPUSH", "list", "a")
	resp = c2.readReply()
	if resp != "QUEUED" {
		t.Fatalf("LPUSH: expected QUEUED, got %q", resp)
	}

	c2.sendCommand("EXEC")
	resp = c2.readReply()
	t.Logf("EXEC result: %q", resp)

	// Wait for BLPOP to complete
	blpopDone.Wait()

	// BLPOP should get "a" — the notification fired after the tx committed
	if blpopResult != "list,a" {
		t.Errorf("BLPOP: expected %q, got %q", "list,a", blpopResult)
	}
}

// TestCorrectness_BLPOP_NonTransactional verifies that BLPOP wakes up
// correctly for non-transactional LPUSH via the effects engine callbacks.
func TestCorrectness_BLPOP_NonTransactional(t *testing.T) {
	addr, cleanup := startRedisTestServerWithEffects(t)
	defer cleanup()

	c1 := newRedisClient(t, addr)
	defer c1.close()
	c2 := newRedisClient(t, addr)
	defer c2.close()

	// Clean slate
	c2.sendCommand("DEL", "list")
	c2.readReply()

	// Client 1: block on BLPOP
	var blpopResult string
	var blpopDone sync.WaitGroup
	blpopDone.Go(func() {
		c1.sendCommand("BLPOP", "list", "5")
		blpopResult = c1.readReply()
	})

	time.Sleep(100 * time.Millisecond)

	// Client 2: simple LPUSH (non-transactional)
	c2.sendCommand("LPUSH", "list", "hello")
	resp := c2.readReply()
	if resp != "1" {
		t.Fatalf("LPUSH: expected 1, got %q", resp)
	}

	blpopDone.Wait()

	if blpopResult != "list,hello" {
		t.Errorf("BLPOP: expected %q, got %q", "list,hello", blpopResult)
	}
}

// TestCorrectness_BLPOP_LPUSH_DEL_SET_ShouldNotAwakeBlockedClient verifies
// BLPOP is not woken by LPUSH+DEL+SET inside MULTI/EXEC.
func TestCorrectness_BLPOP_LPUSH_DEL_SET_ShouldNotAwakeBlockedClient(t *testing.T) {
	addr, cleanup := startRedisTestServerWithEffects(t)
	defer cleanup()

	c1 := newRedisClient(t, addr)
	defer c1.close()
	c2 := newRedisClient(t, addr)
	defer c2.close()

	// Clean slate
	c2.sendCommand("DEL", "list")
	c2.readReply()

	// Client 1: block on BLPOP
	var blpopResult string
	var blpopDone sync.WaitGroup
	blpopDone.Go(func() {
		c1.sendCommand("BLPOP", "list", "5")
		blpopResult = c1.readReply()
	})

	time.Sleep(100 * time.Millisecond)

	// Client 2: MULTI { LPUSH list a, DEL list, SET list notalist } EXEC
	c2.sendCommand("MULTI")
	resp := c2.readReply()
	if resp != "OK" {
		t.Fatalf("MULTI: expected OK, got %q", resp)
	}

	c2.sendCommand("LPUSH", "list", "a")
	resp = c2.readReply()
	if resp != "QUEUED" {
		t.Fatalf("LPUSH: expected QUEUED, got %q", resp)
	}

	c2.sendCommand("DEL", "list")
	resp = c2.readReply()
	if resp != "QUEUED" {
		t.Fatalf("DEL: expected QUEUED, got %q", resp)
	}

	c2.sendCommand("SET", "list", "notalist")
	resp = c2.readReply()
	if resp != "QUEUED" {
		t.Fatalf("SET: expected QUEUED, got %q", resp)
	}

	c2.sendCommand("EXEC")
	resp = c2.readReply()
	t.Logf("EXEC result: %q", resp)

	time.Sleep(100 * time.Millisecond)

	// Now delete the string and push to list
	c2.sendCommand("DEL", "list")
	c2.readReply()

	c2.sendCommand("LPUSH", "list", "b")
	resp = c2.readReply()
	if resp != "1" {
		t.Fatalf("LPUSH b: expected 1, got %q", resp)
	}

	blpopDone.Wait()

	if blpopResult != "list,b" {
		t.Errorf("BLPOP: expected %q, got %q", "list,b", blpopResult)
	}
}
