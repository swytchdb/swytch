package redis

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// Keep the same writer across MULTI and EXEC, as a server connection does.
func commandClient(h *Handler) func(...string) string {
	c := ssiConn(h)

	var buf bytes.Buffer

	w := shared.NewWriter(&buf)

	return func(args ...string) string {
		buf.Reset()

		cmd := &shared.Command{Type: shared.ParseCommandType([]byte(args[0]))}
		for _, arg := range args[1:] {
			cmd.Args = append(cmd.Args, []byte(arg))
		}

		h.ExecuteInto(cmd, w, c)

		return buf.String()
	}
}

type commandPartition struct{ effects.Broadcaster }

func (*commandPartition) InMajorityPartition() bool { return false }
func (*commandPartition) PeerIDs() []pb.NodeID      { return []pb.NodeID{2} }
func (*commandPartition) ReplicateTo(*pb.OffsetNotify, []byte, pb.NodeID) ([]*pb.NackNotify, error) {
	return nil, nil
}

// Inject faults inside the runner so the real dispatcher still owns rollback
// and flush. Support normal, blocking, and transaction preparation paths.
func withCommandFault(h *Handler, kind shared.CommandType, fault func(*shared.Command) func()) {
	entry := *h.cmds.Lookup(kind)
	wrap := func(cmd *shared.Command, run shared.CommandRunner) shared.CommandRunner {
		return func() {
			cleanup := fault(cmd)
			defer cleanup()

			run()
		}
	}

	if handler := entry.Handler; handler != nil {
		entry.Handler = func(cmd *shared.Command, w *shared.Writer, db *shared.Database) (bool, []string, shared.CommandRunner) {
			valid, keys, runner := handler(cmd, w, db)

			return valid, keys, wrap(cmd, runner)
		}
	}

	if handler := entry.ConnHandler; handler != nil {
		entry.ConnHandler = func(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
			wrap(cmd, func() { handler(cmd, w, db, conn) })()
		}
	}

	if prepare := entry.TxnPrepare; prepare != nil {
		entry.TxnPrepare = func(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) (bool, []string, shared.CommandRunner) {
			valid, keys, runner := prepare(cmd, w, db, conn)

			return valid, keys, wrap(cmd, runner)
		}
	}

	h.cmds.Register(kind, &entry)
}

func TestDataCommandRollback(t *testing.T) {
	for _, tc := range []struct {
		name string
		seed []string
		cmd  []string
		read []string
		want string
	}{
		{"MSET", []string{"SET", "source", "old"}, []string{"MSET", "source", "new", "dest", "new"}, []string{"GET", "source"}, "$3\r\nold\r\n"},
		{"SMOVE", []string{"SADD", "source", "member"}, []string{"SMOVE", "source", "dest", "member"}, []string{"SCARD", "source"}, ":1\r\n"},
		{"LMOVE", []string{"RPUSH", "source", "member"}, []string{"LMOVE", "source", "dest", "LEFT", "RIGHT"}, []string{"LLEN", "source"}, ":1\r\n"},
		{"BLMOVE", []string{"RPUSH", "source", "member"}, []string{"BLMOVE", "source", "dest", "LEFT", "RIGHT", "0"}, []string{"LLEN", "source"}, ":1\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range []string{"standalone", "queued", "ssi"} {
				t.Run(mode, func(t *testing.T) {
					h := newSSIHandler(t)
					h.ssi = mode == "ssi"

					run := commandClient(h)
					if reply := run(tc.seed...); strings.HasPrefix(reply, "-") {
						t.Fatal(reply)
					}

					kind := shared.ParseCommandType([]byte(tc.name))
					withCommandFault(h, kind, func(cmd *shared.Command) func() {
						injected := false

						if kind == shared.CmdMSet {
							h.engine.SetBroadcaster(&commandPartition{})

							injected = true
						} else {
							h.engine.SetOnLocalEffect(func(_ effects.Tip, eff *pb.Effect) {
								if !injected && string(eff.GetKey()) == "source" && eff.GetData().GetOp() == pb.EffectOp_REMOVE_OP {
									injected = true

									h.engine.SetBroadcaster(&commandPartition{})
									// The destination's absent read now subscribes before
									// removal. Fail a fresh transactional read explicitly
									// so the next write fails regardless of subscriptions.
									if _, _, err := cmd.Context.GetSnapshot("rollback-fault"); !errors.Is(err, effects.ErrRegionPartitioned) {
										t.Errorf("fault read: got %v, want region partitioned", err)
									}
								}
							})
						}

						return func() {
							h.engine.SetOnLocalEffect(nil)
							h.engine.SetBroadcaster(nil)

							if !injected {
								t.Error("command fault was not injected")
							}
						}
					})

					if mode != "standalone" {
						if reply := run("MULTI"); reply != "+OK\r\n" {
							t.Fatal(reply)
						}
					}

					run("SET", "before", "value")
					reply := run(tc.cmd...)
					run("SET", "after", "value")

					wantError := "-" + effects.ErrRegionPartitioned.Error() + "\r\n"

					if mode == "queued" {
						if reply != "+QUEUED\r\n" {
							t.Fatal(reply)
						}
					} else if reply != wantError {
						t.Fatalf("got %q, want %q", reply, wantError)
					}

					if mode != "standalone" {
						if reply := run("EXEC"); reply != "*3\r\n+OK\r\n"+wantError+"+OK\r\n" {
							t.Fatalf("EXEC: %q", reply)
						}
					}

					if got := run(tc.read...); got != tc.want {
						t.Fatalf("failed command changed source: %q", got)
					}

					if got := run("EXISTS", "dest"); got != ":0\r\n" {
						t.Fatalf("failed command created destination: %q", got)
					}

					for _, key := range []string{"before", "after"} {
						if got := run("GET", key); got != "$5\r\nvalue\r\n" {
							t.Fatalf("lost surrounding write to %s: %q", key, got)
						}
					}
				})
			}
		})
	}
}

func TestMovePartitionPreventsCommit(t *testing.T) {
	for _, tc := range []struct {
		name  string
		seed  []string
		cmd   []string
		read  []string
		reply string
	}{
		{"SMOVE", []string{"SADD", "source", "member"}, []string{"SMOVE", "source", "dest", "member"}, []string{"SMEMBERS", "source"}, ":1\r\n"},
		{"LMOVE", []string{"RPUSH", "source", "member"}, []string{"LMOVE", "source", "dest", "LEFT", "RIGHT"}, []string{"LRANGE", "source", "0", "-1"}, "$6\r\nmember\r\n"},
		{"BLMOVE", []string{"RPUSH", "source", "member"}, []string{"BLMOVE", "source", "dest", "LEFT", "RIGHT", "0"}, []string{"LRANGE", "source", "0", "-1"}, "$6\r\nmember\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range []string{"queued", "ssi"} {
				t.Run(mode, func(t *testing.T) {
					h := newSSIHandler(t)
					h.ssi = mode == "ssi"
					run := commandClient(h)

					if reply := run(tc.seed...); strings.HasPrefix(reply, "-") {
						t.Fatal(reply)
					}

					partitioned, inserted := false, false

					withCommandFault(h, shared.ParseCommandType([]byte(tc.name)), func(_ *shared.Command) func() {
						h.engine.SetOnLocalEffect(func(_ effects.Tip, eff *pb.Effect) {
							if string(eff.GetKey()) == "source" && eff.GetData().GetOp() == pb.EffectOp_REMOVE_OP {
								partitioned = true

								h.engine.SetBroadcaster(&commandPartition{})
							}

							if string(eff.GetKey()) == "dest" && eff.GetData().GetOp() == pb.EffectOp_INSERT_OP {
								inserted = true
							}
						})

						// Keep the partition in place until EXEC has checked it.
						return func() { h.engine.SetOnLocalEffect(nil) }
					})

					if reply := run("MULTI"); reply != "+OK\r\n" {
						t.Fatal(reply)
					}

					run("SET", "before", "value")

					want := "+QUEUED\r\n"
					if mode == "ssi" {
						want = tc.reply
					}

					if reply := run(tc.cmd...); reply != want {
						t.Fatalf("move reply: got %q, want %q", reply, want)
					}

					if reply := run("EXEC"); reply != "-TRYAGAIN "+effects.ErrRegionPartitioned.Error()+"\r\n" {
						t.Fatalf("partitioned transaction committed: %q", reply)
					}

					h.engine.SetBroadcaster(nil)

					if !partitioned || !inserted {
						t.Fatalf("did not exercise provisional destination write during partition: partitioned=%v inserted=%v", partitioned, inserted)
					}

					if got := run(tc.read...); got != "*1\r\n$6\r\nmember\r\n" {
						t.Fatalf("aborted move changed source: %q", got)
					}

					if got := run("EXISTS", "dest", "before"); got != ":0\r\n" {
						t.Fatalf("aborted transaction leaked writes: %q", got)
					}
				})
			}
		})
	}
}

func TestJSONMSetRollback(t *testing.T) {
	for _, ssi := range []bool{false, true} {
		h := newSSIHandler(t)
		run := commandClient(h)
		run("JSON.SET", "a", ".", `{"x":1}`)
		run("JSON.SET", "b", ".", `{}`)

		if ssi {
			run("MULTI")
		}

		reply := run("JSON.MSET", "a", ".", `{"x":2}`, "b", ".missing.child", "1")
		if reply != "-ERR JSON path could not be set\r\n" {
			t.Fatal(reply)
		}

		if ssi {
			if got := run("EXEC"); got != "*1\r\n"+reply {
				t.Fatal(got)
			}
		}

		if got := run("JSON.GET", "a", "."); got != "$7\r\n{\"x\":1}\r\n" {
			t.Fatalf("partial JSON.MSET persisted: %q", got)
		}
	}
}

func TestScriptErrorPreservesSuccessfulCommands(t *testing.T) {
	h := newSSIHandler(t)
	run := commandClient(h)

	reply := run("EVAL", "redis.call('SET', KEYS[1], 'value'); redis.call('LPUSH', KEYS[1], 'item')", "1", "key")
	if !strings.HasPrefix(reply, "-") {
		t.Fatal(reply)
	}

	if got := run("GET", "key"); got != "$5\r\nvalue\r\n" {
		t.Fatalf("script error rolled back successful SET: %q", got)
	}
}

func TestSSIReadFailureStillAbortsTransaction(t *testing.T) {
	h := newSSIHandler(t)
	run := commandClient(h)
	run("MULTI")
	run("SET", "before", "value")
	h.engine.SetCloudReader(delFailingCloudReader{key: "unavailable"})

	reply := run("GET", "unavailable")

	h.engine.SetCloudReader(nil)

	if !strings.HasPrefix(reply, "-") || !strings.HasPrefix(run("EXEC"), "-TRYAGAIN ") {
		t.Fatal("uncertain read must abort the transaction")
	}

	if got := run("GET", "before"); got != "$-1\r\n" {
		t.Fatalf("failed read allowed a commit: %q", got)
	}
}

func TestHashFailureDiscardsPartialReply(t *testing.T) {
	for _, args := range [][]string{
		{"HGETDEL", "h", "FIELDS", "3", "a", "b", "c"},
		{"HEXPIRE", "h", "0", "FIELDS", "3", "a", "b", "c"},
		{"HEXPIRE", "h", "600", "FIELDS", "3", "a", "b", "c"},
		{"HGETEX", "h", "EX", "600", "FIELDS", "3", "a", "b", "c"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			h := newSSIHandler(t)
			run := commandClient(h)
			run("HSET", "h", "a", "one", "b", "two", "c", "three")
			run("MULTI")
			run("HSET", "h", "before", "keep")

			withCommandFault(h, shared.ParseCommandType([]byte(args[0])), func(cmd *shared.Command) func() {
				h.engine.SetCloudReader(delFailingCloudReader{key: "unavailable"})

				injected := false

				h.engine.SetOnLocalEffect(func(_ effects.Tip, eff *pb.Effect) {
					if !injected && string(eff.GetKey()) == "h" && (eff.GetData() != nil || eff.GetMeta() != nil) {
						injected = true
						// A failed transactional read makes the next Emit fail.
						_, _, _ = cmd.Context.GetSnapshot("unavailable")
					}
				})

				return func() {
					h.engine.SetOnLocalEffect(nil)
					h.engine.SetCloudReader(nil)

					if !injected {
						t.Error("fault did not run after a write")
					}
				}
			})

			reply := run(args...)
			if !strings.HasPrefix(reply, "-cloud unavailable:") || strings.Count(reply, "\r\n") != 1 {
				t.Fatalf("partial aggregate escaped: %q", reply)
			}

			if got := run("EXEC"); got != "*2\r\n:1\r\n"+reply {
				t.Fatalf("EXEC lost earlier reply or write: %q", got)
			}

			if got := run("HMGET", "h", "a", "b", "c", "before"); got != "*4\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n$4\r\nkeep\r\n" {
				t.Fatalf("failed command changed fields: %q", got)
			}

			if got := run("HPTTL", "h", "FIELDS", "3", "a", "b", "c"); got != "*3\r\n:-1\r\n:-1\r\n:-1\r\n" {
				t.Fatalf("failed command changed TTLs: %q", got)
			}
		})
	}
}
