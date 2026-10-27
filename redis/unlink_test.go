package redis

import (
	"strings"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/swytch/redis/shared"
)

func unlinkTestClient(h *Handler) func(...string) string {
	conn := ssiConn(h)

	return func(args ...string) string { return ssiCommand(h, conn, args...) }
}

func TestUnlink(t *testing.T) {
	h := newSSIHandler(t)
	h.stats = shared.NewStats()

	run := unlinkTestClient(h)

	if got := run("UNLINK"); got != "-ERR wrong number of arguments for 'unlink' command\r\n" {
		t.Fatal(got)
	}

	for _, seed := range [][]string{
		{"SET", "string", "value", "EX", "600"},
		{"RPUSH", "list", "value"},
		{"HSET", "hash", "field", "value"},
		{"SADD", "set", "value"},
		{"ZADD", "zset", "1", "value"},
		{"JSON.SET", "json", "$", "{}"},
		{"XADD", "stream", "IDMP", "producer", "iid", "*", "field", "value"},
		{"XGROUP", "CREATE", "stream", "group", "0"},
		{"XREADGROUP", "GROUP", "group", "consumer", "STREAMS", "stream", ">"},
		{"SET", "expired", "value"},
		{"PEXPIREAT", "expired", "1"},
	} {
		if got := run(seed...); strings.HasPrefix(got, "-") {
			t.Fatalf("%v: %q", seed, got)
		}
	}

	keys := []string{"string", "list", "hash", "set", "zset", "json", "stream", "missing", "expired", "string"}
	if got := run(append([]string{"uNlInK"}, keys...)...); got != ":7\r\n" {
		t.Fatalf("wrong deletion count: %q", got)
	}

	if got := run(append([]string{"EXISTS"}, keys...)...); got != ":0\r\n" {
		t.Fatalf("keys survived UNLINK: %q", got)
	}

	if got := run(append([]string{"UNLINK"}, keys...)...); got != ":0\r\n" {
		t.Fatalf("repeated UNLINK should delete nothing: %q", got)
	}

	for _, key := range []string{
		"stream\x01g\x01__registry__", "stream\x01g\x01group",
		"stream\x01pel\x01group", "stream\x01p\x01__registry__",
		"stream\x01p\x01producer", "stream\x01cfg",
	} {
		snap, _, _, err := h.engine.GetSnapshot(key)
		if err != nil {
			t.Fatal(err)
		}

		// DEL clears configuration fields individually, leaving an empty
		// KEYED snapshot rather than a whole-key removal.
		if snap != nil && snap.GetOp() != pb.EffectOp_REMOVE_OP && len(snap.GetNetAdds()) != 0 {
			t.Fatalf("UNLINK left live stream metadata at %q", key)
		}
	}

	if got := run("COMMAND", "INFO", "unlink"); !strings.Contains(got, "$6\r\nUNLINK\r\n") {
		t.Fatalf("UNLINK missing from command discovery: %q", got)
	}

	if got := run("INFO", "COMMANDSTATS"); !strings.Contains(got, "cmdstat_unlink:") {
		t.Fatalf("UNLINK missing from command statistics: %q", got)
	}
}

func TestUnlinkFailureRollback(t *testing.T) {
	testDeleteFailureRollback(t, "UNLINK")
}

func TestUnlinkProtocol(t *testing.T) {
	addr, cleanup := startRedisTestServer(t)
	defer cleanup()

	c := newRedisClient(t, addr)
	defer c.close()

	for _, step := range []struct {
		args []string
		want string
	}{
		{[]string{"SET", "key", "value"}, "OK"},
		{[]string{"MULTI"}, "OK"},
		{[]string{"uNlInK", "key", "missing", "key"}, "QUEUED"},
		{[]string{"EXISTS", "key"}, "QUEUED"},
		{[]string{"EXEC"}, "1,0"},
		{[]string{"UNLINK"}, "-ERR wrong number of arguments for 'unlink' command"},
		{[]string{"PING"}, "PONG"},
	} {
		c.sendCommand(step.args...)

		if got := c.readReply(); got != step.want {
			t.Fatalf("%v: got %q, want %q", step.args, got, step.want)
		}
	}
}

func TestUnlinkACL(t *testing.T) {
	h := newSSIHandler(t)
	admin := unlinkTestClient(h)
	admin("SET", "allowed", "value")
	admin("SET", "denied", "value")

	if got := admin("ACL", "SETUSER", "deleter", "on", "nopass", "+unlink", "~allowed"); got != "+OK\r\n" {
		t.Fatal(got)
	}

	run := unlinkTestClient(h)
	if got := run("AUTH", "deleter", ""); got != "+OK\r\n" {
		t.Fatal(got)
	}

	if got := run("UNLINK", "allowed", "denied"); !strings.HasPrefix(got, "-NOPERM ") {
		t.Fatalf("UNLINK must check every key: %q", got)
	}

	if got := admin("EXISTS", "allowed", "denied"); got != ":2\r\n" {
		t.Fatalf("denied UNLINK changed data: %q", got)
	}

	if got := run("DEL", "allowed"); !strings.HasPrefix(got, "-NOPERM ") {
		t.Fatalf("UNLINK permission must not authorize DEL: %q", got)
	}

	if got := run("UNLINK", "allowed"); got != ":1\r\n" {
		t.Fatalf("authorized UNLINK failed: %q", got)
	}
}
