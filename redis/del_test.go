package redis

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// Fail only the chosen missing key so DEL can make progress before the error.
type delFailingCloudReader struct {
	ssiFailingCloudReader

	key string
}

func (r delFailingCloudReader) MayHold(key string) bool { return key == r.key }

func (r delFailingCloudReader) CloudTips(ctx context.Context, key string) ([]effects.Tip, []effects.CloudEffect, []effects.Tip, error) {
	if key == r.key {
		return r.ssiFailingCloudReader.CloudTips(ctx, key)
	}

	return nil, nil, nil, nil
}

func TestDelFailureRollback(t *testing.T) {
	for _, failure := range []struct {
		name string
		key  string
		last string
	}{
		{name: "later key read", key: "unavailable", last: "unavailable"},
		{name: "stream cleanup", key: "stream\x01p\x01__registry__", last: "stream"},
	} {
		t.Run(failure.name, func(t *testing.T) {
			for _, mode := range []string{"standalone", "queued", "ssi"} {
				t.Run(mode, func(t *testing.T) {
					h := newSSIHandler(t)
					h.ssi = mode == "ssi"
					c, other := ssiConn(h), ssiConn(h)
					// Queued runners capture the connection's writer, as on the server.
					var buf bytes.Buffer

					w := shared.NewWriter(&buf)
					run := func(args ...string) string {
						buf.Reset()

						cmd := &shared.Command{Type: shared.ParseCommandType([]byte(args[0]))}
						for _, arg := range args[1:] {
							cmd.Args = append(cmd.Args, []byte(arg))
						}

						h.ExecuteInto(cmd, w, c)

						return buf.String()
					}
					wantReply := func(want string, args ...string) {
						t.Helper()

						if got := run(args...); got != want {
							t.Fatalf("%v: got %q, want %q", args, got, want)
						}
					}

					ssiWant(t, h, c, "+OK\r\n", "SET", "first", "old")
					ssiWant(t, h, c, "$3\r\n1-0\r\n", "XADD", "stream", "1-0", "field", "value")
					ssiWant(t, h, c, "+OK\r\n", "XGROUP", "CREATE", "stream", "group", "0")

					if got := ssiCommand(h, c, "XREADGROUP", "GROUP", "group", "consumer", "STREAMS", "stream", ">"); !strings.HasPrefix(got, "*1\r\n") {
						t.Fatalf("seed pending entry: %q", got)
					}

					groups := ssiCommand(h, c, "XINFO", "GROUPS", "stream")
					pending := ssiCommand(h, c, "XPENDING", "stream", "group")

					if mode != "standalone" {
						wantReply("+OK\r\n", "MULTI")

						want := "+QUEUED\r\n"
						if mode == "ssi" {
							want = "+OK\r\n"
						}

						// Include an earlier write to the same key DEL will remove.
						wantReply(want, "SET", "first", "new")
						wantReply(want, "SET", "before", "value")
					}

					h.engine.SetCloudReader(delFailingCloudReader{key: failure.key})

					reply := run("DEL", "first", failure.last)
					if mode == "queued" {
						if reply != "+QUEUED\r\n" {
							t.Fatalf("queue DEL: %q", reply)
						}

						reply = run("EXEC")

						prefix := "*3\r\n+OK\r\n+OK\r\n"
						if !strings.HasPrefix(reply, prefix) {
							t.Fatalf("EXEC lost earlier results: %q", reply)
						}

						reply = strings.TrimPrefix(reply, prefix)
					}

					if !strings.HasPrefix(reply, "-") || !strings.Contains(reply, effects.ErrCloudUnavailable.Error()) {
						t.Fatalf("expected cloud error, got %q", reply)
					}

					h.engine.SetCloudReader(nil)

					if mode == "ssi" {
						ssiWant(t, h, other, "$3\r\nold\r\n", "GET", "first")
						ssiWant(t, h, other, "$-1\r\n", "GET", "before")
						ssiWant(t, h, c, "$3\r\nnew\r\n", "GET", "first")
						ssiWant(t, h, c, "+OK\r\n", "SET", "after", "value")
						ssiWant(t, h, c, "*5\r\n+OK\r\n+OK\r\n"+reply+"$3\r\nnew\r\n+OK\r\n", "EXEC")
						ssiWant(t, h, other, "$5\r\nvalue\r\n", "GET", "after")
					}

					want := "$3\r\nold\r\n"
					if mode != "standalone" {
						want = "$3\r\nnew\r\n"

						ssiWant(t, h, other, "$5\r\nvalue\r\n", "GET", "before")
					}

					ssiWant(t, h, other, want, "GET", "first")
					ssiWant(t, h, other, ":1\r\n", "XLEN", "stream")
					ssiWant(t, h, other, groups, "XINFO", "GROUPS", "stream")
					ssiWant(t, h, other, pending, "XPENDING", "stream", "group")
					// Once the failure is gone the same deletion can be retried.
					ssiWant(t, h, other, ":2\r\n", "DEL", "first", "stream", "first")
					ssiWant(t, h, other, ":0\r\n", "EXISTS", "first", "stream")
				})
			}
		})
	}
}
