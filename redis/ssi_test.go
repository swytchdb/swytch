package redis

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

func newSSIHandler(t *testing.T) *Handler {
	t.Helper()

	engine := effects.NewTestEngine()
	h := NewHandler(HandlerConfig{Engine: engine, SSI: true})

	t.Cleanup(func() {
		h.Close()

		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	})

	return h
}

// Include both contexts, like a real server connection (older test helpers
// only create EffectsCtx and cannot catch WATCH/MULTI context routing bugs).
func ssiConn(h *Handler) *shared.Connection {
	return &shared.Connection{
		Ctx:         context.Background(),
		EffectsCtx:  h.engine.NewContext(),
		ReadOnlyCtx: h.engine.NewReadOnlyContext(),
	}
}

func ssiCommand(h *Handler, conn *shared.Connection, args ...string) string {
	var buf bytes.Buffer

	w := shared.NewWriter(&buf)
	w.SetProtocol(conn.Protocol)

	cmd := &shared.Command{Type: shared.ParseCommandType([]byte(args[0]))}
	for _, arg := range args[1:] {
		cmd.Args = append(cmd.Args, []byte(arg))
	}

	h.ExecuteInto(cmd, w, conn)

	return buf.String()
}

func ssiWant(t *testing.T, h *Handler, conn *shared.Connection, want string, args ...string) {
	t.Helper()

	if got := ssiCommand(h, conn, args...); got != want {
		t.Fatalf("%v: got %q, want %q", args, got, want)
	}
}

func TestSSIImmediateResultsAndCommit(t *testing.T) {
	h := newSSIHandler(t)
	c, other := ssiConn(h), ssiConn(h)
	ssiWant(t, h, c, "+OK\r\n", "MULTI")
	ssiWant(t, h, c, ":1\r\n", "INCR", "count")
	ssiWant(t, h, c, "$1\r\n1\r\n", "GET", "count")
	ssiWant(t, h, other, "$-1\r\n", "GET", "count")
	ssiWant(t, h, c, "*2\r\n:1\r\n$1\r\n1\r\n", "EXEC")
	ssiWant(t, h, other, "$1\r\n1\r\n", "GET", "count")
	ssiWant(t, h, c, "+OK\r\n", "MULTI")
	ssiWant(t, h, c, "*0\r\n", "EXEC")
}

func TestSSIRESP3(t *testing.T) {
	h := newSSIHandler(t)
	c := ssiConn(h)
	c.Protocol = shared.RESP3
	ssiWant(t, h, c, "+OK\r\n", "MULTI")
	ssiWant(t, h, c, "_\r\n", "GET", "missing")
	ssiWant(t, h, c, "*1\r\n_\r\n", "EXEC")
}

func TestSSISnapshotStartsAtMultiAndConflictsAbort(t *testing.T) {
	h := newSSIHandler(t)
	c, other := ssiConn(h), ssiConn(h)
	ssiWant(t, h, other, "+OK\r\n", "SET", "key", "old")
	ssiWant(t, h, c, "+OK\r\n", "MULTI")
	ssiWant(t, h, other, "+OK\r\n", "SET", "key", "new")
	ssiWant(t, h, other, "+OK\r\n", "SET", "missing", "new")
	ssiWant(t, h, c, "$3\r\nold\r\n", "GET", "key")
	ssiWant(t, h, c, "$-1\r\n", "GET", "missing")
	ssiWant(t, h, c, "+OK\r\n", "SET", "key", "mine")
	ssiWant(t, h, c, "+OK\r\n", "SET", "unrelated", "mine")
	ssiWant(t, h, c, "*-1\r\n", "EXEC")
	ssiWant(t, h, other, "$3\r\nnew\r\n", "GET", "key")
	ssiWant(t, h, other, "$-1\r\n", "GET", "unrelated")
	ssiWant(t, h, c, "+OK\r\n", "MULTI")
	ssiWant(t, h, c, ":1\r\n", "INCR", "fresh")
	ssiWant(t, h, c, "*1\r\n:1\r\n", "EXEC")
}

func TestSSIDiscardAndValidationAbort(t *testing.T) {
	for _, finish := range []string{"DISCARD", "invalid", "unknown"} {
		t.Run(finish, func(t *testing.T) {
			h := newSSIHandler(t)
			c, other := ssiConn(h), ssiConn(h)
			ssiWant(t, h, c, "+OK\r\n", "MULTI")
			ssiWant(t, h, c, "+OK\r\n", "SET", "key", "pending")

			if finish == "DISCARD" {
				ssiWant(t, h, c, "+OK\r\n", "DISCARD")
			} else {
				cmd := []string{"SET"}
				if finish == "unknown" {
					cmd = []string{"NOTACOMMAND"}
				}

				if got := ssiCommand(h, c, cmd...); !strings.HasPrefix(got, "-ERR") {
					t.Fatalf("expected command error, got %q", got)
				}

				if got := ssiCommand(h, c, "EXEC"); !strings.HasPrefix(got, "-EXECABORT") {
					t.Fatalf("expected EXECABORT, got %q", got)
				}
			}

			ssiWant(t, h, other, "$-1\r\n", "GET", "key")
			ssiWant(t, h, c, "+OK\r\n", "SET", "next", "ok")
			ssiWant(t, h, c, "$-1\r\n", "GET", "key")
		})
	}
}

func TestSSIWatchAbortDoesNotPublishPendingWrites(t *testing.T) {
	for _, ssi := range []bool{false, true} {
		h := newSSIHandler(t)
		h.ssi = ssi
		c, other := ssiConn(h), ssiConn(h)
		ssiWant(t, h, c, "+OK\r\n", "WATCH", "watched")
		ssiWant(t, h, c, "+OK\r\n", "MULTI")

		want := "+QUEUED\r\n"
		if ssi {
			want = "+OK\r\n"
		}

		ssiWant(t, h, c, want, "SET", "pending", "value")
		ssiWant(t, h, other, "+OK\r\n", "SET", "watched", "changed")
		ssiWant(t, h, c, "*-1\r\n", "EXEC")
		ssiWant(t, h, other, "$-1\r\n", "GET", "pending")
		ssiWant(t, h, c, "+OK\r\n", "SET", "next", "value")
		ssiWant(t, h, other, "$-1\r\n", "GET", "pending")
	}
}

func TestSSIRuntimeErrorAndBlockingFallback(t *testing.T) {
	h := newSSIHandler(t)
	c := ssiConn(h)
	ssiWant(t, h, c, "+OK\r\n", "MULTI")
	ssiWant(t, h, c, "+OK\r\n", "SET", "str", "value")

	errReply := ssiCommand(h, c, "LPUSH", "str", "item")
	if !strings.HasPrefix(errReply, "-WRONGTYPE") {
		t.Fatalf("got %q", errReply)
	}

	ssiWant(t, h, c, "*-1\r\n", "BLPOP", "empty", "0")
	ssiWant(t, h, c, "*3\r\n+OK\r\n"+errReply+"*-1\r\n", "EXEC")
	ssiWant(t, h, c, "$5\r\nvalue\r\n", "GET", "str")
}

func TestSSIScriptUsesSnapshotAndProducesOneResult(t *testing.T) {
	h := newSSIHandler(t)
	c, other := ssiConn(h), ssiConn(h)
	ssiWant(t, h, c, "+OK\r\n", "MULTI")
	ssiWant(t, h, c, ":1\r\n", "EVAL", "return redis.call('INCR', KEYS[1])", "1", "count")
	ssiWant(t, h, other, "$-1\r\n", "GET", "count")
	ssiWant(t, h, c, "*1\r\n:1\r\n", "EXEC")
	ssiWant(t, h, other, "$1\r\n1\r\n", "GET", "count")
}

func TestSSIRejectsNonTransactionalCommands(t *testing.T) {
	h := newSSIHandler(t)
	c := ssiConn(h)

	for _, cmd := range [][]string{{"FLUSHALL"}, {"FLUSHDB"}, {"PUBLISH", "ch", "msg"}, {"SUBSCRIBE", "ch"}, {"KEYS", "*"}, {"SCAN", "0"}, {"RANDOMKEY"}, {"DBSIZE"}} {
		ssiWant(t, h, c, "+OK\r\n", "SET", "existing", "value")
		ssiWant(t, h, c, "+OK\r\n", "MULTI")
		ssiWant(t, h, c, "+OK\r\n", "SET", "pending", "value")

		if got := ssiCommand(h, c, cmd...); !strings.HasPrefix(got, "-ERR") {
			t.Fatalf("%v: %q", cmd, got)
		}

		if got := ssiCommand(h, c, "EXEC"); !strings.HasPrefix(got, "-EXECABORT") {
			t.Fatalf("%q", got)
		}

		ssiWant(t, h, c, "$5\r\nvalue\r\n", "GET", "existing")
		ssiWant(t, h, c, "$-1\r\n", "GET", "pending")
	}
}

func TestSSIPermissions(t *testing.T) {
	h := newSSIHandler(t)

	for _, cmd := range [][]string{{"GET", "secret"}, {"SET", "secret", "x"}, {"EVAL", "return 1", "1", "secret"}, {"SET", "__swytch:internal", "x"}} {
		c := ssiConn(h)
		c.User = &shared.ACLUser{
			Name: "restricted", Enabled: true, AllCommands: true,
			KeyPatterns: []shared.KeyPattern{{Pattern: "allowed:*", Type: shared.KeyPatternReadWrite}},
		}

		if strings.HasPrefix(cmd[1], "__swytch:") {
			c.User.AllKeys = true
		}

		ssiWant(t, h, c, "+OK\r\n", "MULTI")
		ssiWant(t, h, c, "+OK\r\n", "SET", "allowed:pending", "x")

		if got := ssiCommand(h, c, cmd...); !strings.HasPrefix(got, "-") {
			t.Fatalf("%v: %q", cmd, got)
		}

		if got := ssiCommand(h, c, "EXEC"); !strings.HasPrefix(got, "-EXECABORT") {
			t.Fatalf("%q", got)
		}

		ssiWant(t, h, c, "$-1\r\n", "GET", "allowed:pending")
	}
}

type ssiFailingCloudReader struct{}

func (ssiFailingCloudReader) MayHold(string) bool { return true }
func (ssiFailingCloudReader) MarkPending(string)  {}
func (ssiFailingCloudReader) CloudTips(context.Context, string) ([]effects.Tip, []effects.CloudEffect, []effects.Tip, error) {
	return nil, nil, nil, errors.New("injected cloud failure")
}

func TestSSIFailedRenamePreservesSnapshotAndEarlierWrites(t *testing.T) {
	h := newSSIHandler(t)
	c, other := ssiConn(h), ssiConn(h)
	ssiWant(t, h, c, "+OK\r\n", "SET", "source", "old")
	ssiWant(t, h, c, "+OK\r\n", "MULTI")
	ssiWant(t, h, c, "+OK\r\n", "SET", "before", "value")
	h.engine.SetCloudReader(ssiFailingCloudReader{})

	errReply := ssiCommand(h, c, "RENAME", "source", "unavailable")
	if !strings.HasPrefix(errReply, "-") {
		t.Fatalf("expected failure, got %q", errReply)
	}

	h.engine.SetCloudReader(nil)
	ssiWant(t, h, other, "+OK\r\n", "SET", "source", "new")
	ssiWant(t, h, c, "$3\r\nold\r\n", "GET", "source")
	ssiWant(t, h, c, "$5\r\nvalue\r\n", "GET", "before")
	ssiWant(t, h, c, "+OK\r\n", "DISCARD")
	ssiWant(t, h, other, "$-1\r\n", "GET", "before")
}

func TestSSIServerPipelineAndDisconnect(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.SSI = true

	s, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}

	h := s.handler.(*Handler)
	h.engine = effects.NewTestEngine()

	t.Cleanup(func() {
		s.cancel()
		h.Close()

		if err := h.engine.Close(); err != nil {
			t.Error(err)
		}
	})

	serverConn, clientConn := net.Pipe()

	s.wg.Add(1)
	go s.handleConnection(serverConn)

	t.Cleanup(func() {
		_ = clientConn.Close()

		s.wg.Wait()
	})

	if err := clientConn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	input := "MULTI\r\nSET key value\r\nGET key\r\nINCR count\r\nEXEC\r\nGET count\r\nMULTI\r\nSET abandoned value\r\n"
	want := "+OK\r\n+OK\r\n$5\r\nvalue\r\n:1\r\n*3\r\n+OK\r\n$5\r\nvalue\r\n:1\r\n$1\r\n1\r\n+OK\r\n+OK\r\n"

	if _, err := io.WriteString(clientConn, input); err != nil {
		t.Fatal(err)
	}

	got := make([]byte, len(want))
	if _, err := io.ReadFull(clientConn, got); err != nil {
		t.Fatal(err)
	}

	if string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	if err := clientConn.Close(); err != nil {
		t.Fatal(err)
	}

	s.wg.Wait()
	ssiWant(t, h, ssiConn(h), "$-1\r\n", "GET", "abandoned")
}
