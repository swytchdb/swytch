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

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/swytchdb/swytch/redis/shared"
)

func TestACLManager_DefaultUser(t *testing.T) {
	m := NewACLManager()

	// Default user should exist and have nopass
	user := m.GetUser("default")
	if user == nil {
		t.Fatal("default user should exist")
	}
	if !user.Enabled {
		t.Error("default user should be enabled")
	}
	if !user.NoPass {
		t.Error("default user should have nopass")
	}
	if !user.AllCommands {
		t.Error("default user should have all commands")
	}
	if !user.AllKeys {
		t.Error("default user should have all keys")
	}
}

func TestACLManager_Authenticate(t *testing.T) {
	m := NewACLManager()

	// Authenticate with default user (nopass)
	user, err := m.Authenticate("default", "")
	if err != nil {
		t.Errorf("should authenticate default user with empty password: %v", err)
	}
	if user.Name != "default" {
		t.Errorf("authenticated user should be default, got %s", user.Name)
	}

	// Create user with password
	m.SetUser(&shared.ACLUser{
		Name:        "alice",
		Enabled:     true,
		Categories:  make(map[shared.CommandCategory]bool),
		Commands:    make(map[shared.CommandType]bool),
		Subcommands: make(map[shared.CommandType]map[string]bool),
	})
	alice := m.GetUser("alice")
	hash, _ := HashPassword("secret123")
	alice.PasswordHashes = [][]byte{hash}

	// Authenticate with correct password
	user, err = m.Authenticate("alice", "secret123")
	if err != nil {
		t.Errorf("should authenticate alice with correct password: %v", err)
	}
	if user.Name != "alice" {
		t.Errorf("authenticated user should be alice, got %s", user.Name)
	}

	// Authenticate with wrong password
	_, err = m.Authenticate("alice", "wrongpassword")
	if err == nil {
		t.Error("should fail with wrong password")
	}

	// Authenticate non-existent user
	_, err = m.Authenticate("bob", "anything")
	if err == nil {
		t.Error("should fail with non-existent user")
	}

	// Authenticate disabled user
	alice.Enabled = false
	_, err = m.Authenticate("alice", "secret123")
	if err == nil {
		t.Error("should fail with disabled user")
	}
}

func TestACLManager_CheckCommand(t *testing.T) {
	m := NewACLManager()

	// Create user with limited permissions
	user := &shared.ACLUser{
		Name:    "reader",
		Enabled: true,
		Categories: map[shared.CommandCategory]bool{
			shared.CatRead: true,
		},
		Commands:    make(map[shared.CommandType]bool),
		Subcommands: make(map[shared.CommandType]map[string]bool),
	}
	m.SetUser(user)

	// Should allow read commands
	if err := m.CheckCommand(user, shared.CmdGet); err != nil {
		t.Errorf("should allow GET for reader: %v", err)
	}

	// Should deny write commands
	if err := m.CheckCommand(user, shared.CmdSet); err == nil {
		t.Error("should deny SET for reader")
	}

	// Test explicit command override
	user.Commands[shared.CmdSet] = true
	if err := m.CheckCommand(user, shared.CmdSet); err != nil {
		t.Errorf("should allow SET after explicit +set: %v", err)
	}

	// Test AllCommands user
	admin := &shared.ACLUser{
		Name:        "admin",
		Enabled:     true,
		AllCommands: true,
	}
	if err := m.CheckCommand(admin, shared.CmdFlushAll); err != nil {
		t.Errorf("should allow FLUSHALL for admin: %v", err)
	}
}

func TestACLManager_CheckKey(t *testing.T) {
	m := NewACLManager()

	// User with specific key pattern
	user := &shared.ACLUser{
		Name:    "app",
		Enabled: true,
		KeyPatterns: []shared.KeyPattern{
			{Pattern: "app:*", Type: shared.KeyPatternReadWrite},
			{Pattern: "readonly:*", Type: shared.KeyPatternReadOnly},
			{Pattern: "writeonly:*", Type: shared.KeyPatternWriteOnly},
		},
	}
	m.SetUser(user)

	// Should allow read/write to app:* keys
	if err := m.CheckKey(user, "app:user:123", false); err != nil {
		t.Errorf("should allow read app:user:123: %v", err)
	}
	if err := m.CheckKey(user, "app:user:123", true); err != nil {
		t.Errorf("should allow write app:user:123: %v", err)
	}

	// Should allow read but not write to readonly:*
	if err := m.CheckKey(user, "readonly:data", false); err != nil {
		t.Errorf("should allow read readonly:data: %v", err)
	}
	if err := m.CheckKey(user, "readonly:data", true); err == nil {
		t.Error("should deny write readonly:data")
	}

	// Should allow write but not read to writeonly:*
	if err := m.CheckKey(user, "writeonly:log", true); err != nil {
		t.Errorf("should allow write writeonly:log: %v", err)
	}
	if err := m.CheckKey(user, "writeonly:log", false); err == nil {
		t.Error("should deny read writeonly:log")
	}

	// Should deny access to other keys
	if err := m.CheckKey(user, "other:key", false); err == nil {
		t.Error("should deny access to other:key")
	}

	// AllKeys user should have access to everything
	admin := &shared.ACLUser{
		Name:    "admin",
		Enabled: true,
		AllKeys: true,
	}
	if err := m.CheckKey(admin, "any:key", true); err != nil {
		t.Errorf("admin should have access to any key: %v", err)
	}
}

func TestACLManager_ApplyRules(t *testing.T) {
	m := NewACLManager()

	user := &shared.ACLUser{
		Name:        "test",
		Categories:  make(map[shared.CommandCategory]bool),
		Commands:    make(map[shared.CommandType]bool),
		Subcommands: make(map[shared.CommandType]map[string]bool),
	}
	m.SetUser(user)

	// Test various rules
	rules := []string{
		"on",           // Enable user
		">password123", // Set password
		"+@read",       // Add read category
		"-@dangerous",  // Remove dangerous category
		"+set",         // Add SET command
		"~app:*",       // Key pattern
		"&channel:*",   // Channel pattern
	}

	if err := m.applyACLRules(user, rules); err != nil {
		t.Fatalf("failed to apply rules: %v", err)
	}

	if !user.Enabled {
		t.Error("user should be enabled")
	}
	if len(user.PasswordHashes) != 1 {
		t.Error("user should have one password")
	}
	if !user.Categories[shared.CatRead] {
		t.Error("user should have read category")
	}
	if user.Categories[shared.CatDangerous] {
		t.Error("user should not have dangerous category")
	}
	if !user.Commands[shared.CmdSet] {
		t.Error("user should have SET command")
	}
	if len(user.KeyPatterns) != 1 || user.KeyPatterns[0].Pattern != "app:*" {
		t.Error("user should have app:* key pattern")
	}
	if len(user.ChannelPatterns) != 1 || user.ChannelPatterns[0] != "channel:*" {
		t.Error("user should have channel:* pattern")
	}
}

func TestACLManager_LoadSaveFile(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()
	aclFile := filepath.Join(tmpDir, "acl.conf")

	// Write initial ACL file
	content := `user default on nopass ~* &* +@all
user alice on >secret123 ~app:* +@read +@write
user bob off >password ~admin:* +@all
`
	if err := os.WriteFile(aclFile, []byte(content), 0600); err != nil {
		t.Fatalf("failed to write ACL file: %v", err)
	}

	// Load ACL file
	m := NewACLManager()
	if err := m.LoadFromFile(aclFile); err != nil {
		t.Fatalf("failed to load ACL file: %v", err)
	}

	// Verify users
	defaultUser := m.GetUser("default")
	if defaultUser == nil || !defaultUser.Enabled || !defaultUser.NoPass {
		t.Error("default user not loaded correctly")
	}

	alice := m.GetUser("alice")
	if alice == nil {
		t.Fatal("alice user not loaded")
	}
	if !alice.Enabled {
		t.Error("alice should be enabled")
	}
	if len(alice.PasswordHashes) != 1 {
		t.Error("alice should have one password")
	}

	bob := m.GetUser("bob")
	if bob == nil {
		t.Fatal("bob user not loaded")
	}
	if bob.Enabled {
		t.Error("bob should be disabled")
	}

	// Test save
	saveFile := filepath.Join(tmpDir, "acl-save.conf")
	m.aclFile = saveFile
	if err := m.SaveToFile(""); err != nil {
		t.Fatalf("failed to save ACL file: %v", err)
	}

	// Reload and verify
	m2 := NewACLManager()
	if err := m2.LoadFromFile(saveFile); err != nil {
		t.Fatalf("failed to reload ACL file: %v", err)
	}

	users := m2.GetUsers()
	if len(users) != 3 {
		t.Errorf("expected 3 users, got %d", len(users))
	}
}

func TestACLManager_GlobMatching(t *testing.T) {
	tests := []struct {
		pattern string
		str     string
		want    bool
	}{
		{"*", "anything", true},
		{"*", "", true},
		{"foo", "foo", true},
		{"foo", "bar", false},
		{"foo*", "foobar", true},
		{"foo*", "foo", true},
		{"foo*", "barfoo", false},
		{"*bar", "foobar", true},
		{"*bar", "bar", true},
		{"*bar", "barbaz", false},
		{"foo*bar", "foobar", true},
		{"foo*bar", "fooxyzbar", true},
		{"foo*bar", "fooxyzbaz", false},
		{"f?o", "foo", true},
		{"f?o", "fao", true},
		{"f?o", "fo", false},
		{"app:*:data", "app:user:data", true},
		{"app:*:data", "app:user:config", false},
		{"[abc]", "a", true},
		{"[abc]", "d", false},
		{"[a-z]", "m", true},
		{"[a-z]", "5", false},
	}

	for _, tt := range tests {
		got := aclMatchGlob(tt.pattern, tt.str)
		if got != tt.want {
			t.Errorf("matchGlob(%q, %q) = %v, want %v", tt.pattern, tt.str, got, tt.want)
		}
	}
}

func TestACLManager_SecurityLog(t *testing.T) {
	m := NewACLManager()

	// Log some denials
	m.LogDenied("command", "toplevel", "FLUSHALL", "alice", "127.0.0.1:12345")
	m.LogDenied("key", "toplevel", "admin:secret", "bob", "127.0.0.1:23456")
	m.LogDenied("command", "toplevel", "FLUSHALL", "alice", "127.0.0.1:12345") // duplicate

	// Get log entries
	entries := m.GetLog(10)
	if len(entries) != 2 {
		t.Errorf("expected 2 log entries, got %d", len(entries))
	}

	// Find the FLUSHALL entry - it should have count 2 (duplicate)
	var flushEntry *ACLLogEntry
	for i := range entries {
		if entries[i].Object == "FLUSHALL" {
			flushEntry = &entries[i]
			break
		}
	}
	if flushEntry == nil {
		t.Error("FLUSHALL entry not found")
	} else if flushEntry.Count != 2 {
		t.Errorf("expected FLUSHALL count 2, got %d", flushEntry.Count)
	}

	// Reset log
	m.ResetLog()
	entries = m.GetLog(10)
	if len(entries) != 0 {
		t.Errorf("expected 0 entries after reset, got %d", len(entries))
	}
}

func TestHandler_ACLCommands(t *testing.T) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 1000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	conn := &shared.Connection{
		SelectedDB: 0,
		User:       testUser,
		Ctx:        context.Background(),
	}

	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)

	// Test ACL WHOAMI
	buf.Reset()
	cmd := &shared.Command{Type: shared.CmdAcl, Args: [][]byte{[]byte("WHOAMI")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("default")) {
		t.Errorf("ACL WHOAMI should return default, got %s", buf.String())
	}

	// Test ACL GENPASS
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdAcl, Args: [][]byte{[]byte("GENPASS")}}
	h.ExecuteInto(cmd, w, conn)
	if buf.Len() < 20 { // Should generate a reasonably long password
		t.Errorf("ACL GENPASS should return a password, got %s", buf.String())
	}

	// Test ACL CAT - list categories
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdAcl, Args: [][]byte{[]byte("CAT")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("read")) {
		t.Errorf("ACL CAT should include 'read' category, got %s", buf.String())
	}

	// Test ACL SETUSER
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdAcl, Args: [][]byte{[]byte("SETUSER"), []byte("testuser"), []byte("on"), []byte(">testpass"), []byte("~test:*"), []byte("+get")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("+OK")) {
		t.Errorf("ACL SETUSER should return OK, got %s", buf.String())
	}

	// Test ACL GETUSER
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdAcl, Args: [][]byte{[]byte("GETUSER"), []byte("testuser")}}
	h.ExecuteInto(cmd, w, conn)
	resp := buf.String()
	if !strings.Contains(resp, "flags") {
		t.Errorf("ACL GETUSER should return user info, got %s", resp)
	}

	// Test ACL USERS
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdAcl, Args: [][]byte{[]byte("USERS")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("default")) || !bytes.Contains(buf.Bytes(), []byte("testuser")) {
		t.Errorf("ACL USERS should list users, got %s", buf.String())
	}

	// Test ACL LIST
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdAcl, Args: [][]byte{[]byte("LIST")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("user default")) {
		t.Errorf("ACL LIST should include user rules, got %s", buf.String())
	}

	// Test ACL DELUSER
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdAcl, Args: [][]byte{[]byte("DELUSER"), []byte("testuser")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte(":1")) {
		t.Errorf("ACL DELUSER should return 1, got %s", buf.String())
	}

	// Verify user was deleted
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdAcl, Args: [][]byte{[]byte("GETUSER"), []byte("testuser")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("$-1")) && !bytes.Contains(buf.Bytes(), []byte("_\r\n")) {
		t.Errorf("deleted user should return nil, got %s", buf.String())
	}

	// Test ACL HELP
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdAcl, Args: [][]byte{[]byte("HELP")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("SETUSER")) {
		t.Errorf("ACL HELP should include command info, got %s", buf.String())
	}
}

func TestHandler_Authentication(t *testing.T) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 1000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	// Set up a user with a password
	h.aclManager.SetUser(&shared.ACLUser{
		Name:        "testuser",
		Enabled:     true,
		AllCommands: true,
		AllKeys:     true,
		AllChannels: true,
		Categories:  make(map[shared.CommandCategory]bool),
		Commands:    make(map[shared.CommandType]bool),
		Subcommands: make(map[shared.CommandType]map[string]bool),
	})
	testUserACL := h.aclManager.GetUser("testuser")
	hash, _ := HashPassword("secret")
	testUserACL.PasswordHashes = [][]byte{hash}

	// Also set default user to require password
	defaultUser := h.aclManager.GetUser("default")
	defaultUser.NoPass = false
	defHash, _ := HashPassword("defaultpass")
	defaultUser.PasswordHashes = [][]byte{defHash}

	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)

	// Test unauthenticated connection
	conn := &shared.Connection{
		SelectedDB: 0,
		User:       nil, // Not authenticated
		Ctx:        context.Background(),
	}

	// Should reject commands other than AUTH
	buf.Reset()
	cmd := &shared.Command{Type: shared.CmdGet, Args: [][]byte{[]byte("foo")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("NOAUTH")) {
		t.Errorf("should require auth, got %s", buf.String())
	}

	// AUTH with default user
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdAuth, Args: [][]byte{[]byte("defaultpass")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("+OK")) {
		t.Errorf("AUTH should succeed, got %s", buf.String())
	}
	if conn.User == nil || conn.User.Name != "default" {
		t.Error("connection should be authenticated as default user")
	}

	// Reset connection
	conn.User = nil

	// AUTH with username and password
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdAuth, Args: [][]byte{[]byte("testuser"), []byte("secret")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("+OK")) {
		t.Errorf("AUTH should succeed, got %s", buf.String())
	}
	if conn.User == nil || conn.User.Name != "testuser" {
		t.Error("connection should be authenticated as testuser")
	}

	// AUTH with wrong password
	conn.User = nil
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdAuth, Args: [][]byte{[]byte("testuser"), []byte("wrongpass")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("WRONGPASS")) {
		t.Errorf("AUTH should fail with wrong password, got %s", buf.String())
	}
}

func TestHandler_CommandACL(t *testing.T) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 1000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	// Create a user with limited permissions
	limitedUser := &shared.ACLUser{
		Name:        "limited",
		Enabled:     true,
		NoPass:      true,
		AllKeys:     true,
		AllChannels: true,
		Categories:  map[shared.CommandCategory]bool{shared.CatRead: true},
		Commands:    make(map[shared.CommandType]bool),
		Subcommands: make(map[shared.CommandType]map[string]bool),
	}
	h.aclManager.SetUser(limitedUser)

	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)

	conn := &shared.Connection{
		SelectedDB: 0,
		User:       limitedUser,
		Username:   "limited",
		Ctx:        context.Background(),
	}

	// Should allow GET (read command)
	buf.Reset()
	cmd := &shared.Command{Type: shared.CmdGet, Args: [][]byte{[]byte("foo")}}
	h.ExecuteInto(cmd, w, conn)
	// GET returns null for non-existent key, not NOPERM
	if bytes.Contains(buf.Bytes(), []byte("NOPERM")) {
		t.Errorf("GET should be allowed, got %s", buf.String())
	}

	// Should deny SET (write command)
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("foo"), []byte("bar")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("NOPERM")) {
		t.Errorf("SET should be denied, got %s", buf.String())
	}

	// Should deny FLUSHDB (dangerous command)
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdFlushDB, Args: nil}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("NOPERM")) {
		t.Errorf("FLUSHDB should be denied, got %s", buf.String())
	}
}
