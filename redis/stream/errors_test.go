package stream

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// Existing keys remain writable, but subscribing to a new key fails the
// engine's majority check. This lets a multi-key command fail after its first
// writes, without replacing any production emit helpers.
type partitionedBroadcaster struct {
	effects.Broadcaster
}

func (*partitionedBroadcaster) InMajorityPartition() bool { return false }
func (*partitionedBroadcaster) PeerIDs() []pb.NodeID      { return []pb.NodeID{2} }
func (*partitionedBroadcaster) ReplicateTo(*pb.OffsetNotify, []byte, pb.NodeID) ([]*pb.NackNotify, error) {
	return nil, nil
}

func runWithoutFlush(t *testing.T, env *testEnv, handler shared.HandlerFunc, args ...string) {
	t.Helper()

	cmd := streamTestCommand(env, args...)

	valid, _, runner := handler(cmd, env.w, nil)
	if !valid || runner == nil {
		t.Fatalf("command rejected before execution: %q", env.buf.String())
	}

	runner()
}

func streamTestCommand(env *testEnv, args ...string) *shared.Command {
	cmd := &shared.Command{Context: env.ctx, Runtime: env.eng}
	for _, arg := range args {
		cmd.Args = append(cmd.Args, []byte(arg))
	}

	return cmd
}

func TestStreamWriteFailureRollback(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T, *testEnv)
	}{
		{"XADD", func(t *testing.T, env *testEnv) {
			runWithoutFlush(t, env, handleXAdd, "s", "IDMP", "producer", "iid", "*", "f", "v")
		}},
		{"XREADGROUP transaction runner", func(t *testing.T, env *testEnv) {
			runWithoutFlush(t, env, handleXReadGroupNonBlocking, "GROUP", "g", "c", "STREAMS", "s", ">")
		}},
		{"XREADGROUP connection handler", func(_ *testing.T, env *testEnv) {
			cmd := streamTestCommand(env, "GROUP", "g", "c", "BLOCK", "1000", "STREAMS", "s", ">")
			handleXReadGroup(cmd, env.w, shared.NewDatabaseManager().DB(), &shared.Connection{Ctx: context.Background()})
		}},
		{"XREADGROUP CLAIM", func(_ *testing.T, env *testEnv) {
			cmd := streamTestCommand(env, "GROUP", "g", "c", "CLAIM", "0", "STREAMS", "s", ">")
			handleXReadGroup(cmd, env.w, nil, &shared.Connection{})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, inTransaction := range []bool{false, true} {
				name := "standalone"
				if inTransaction {
					name = "transaction"
				}

				t.Run(name, func(t *testing.T) {
					env := newTestEnv()
					if got := xadd(t, env, "s", "1-0", "f", "v"); got != "$3\r\n1-0\r\n" {
						t.Fatalf("seed stream: %q", got)
					}

					if got := execHandler(t, handleXGroup, env, "CREATE", "s", "g", "0"); got != "+OK\r\n" {
						t.Fatalf("seed group: %q", got)
					}

					env.buf.Reset()

					cmd := streamTestCommand(env)
					if inTransaction {
						env.ctx.BeginTx()

						if err := emitStreamInsert(cmd, "before", shared.StreamID{Ms: 1}, nil); err != nil {
							t.Fatal(err)
						}
					}

					env.eng.SetBroadcaster(&partitionedBroadcaster{})
					tc.run(t, env)

					want := "-" + effects.ErrRegionPartitioned.Error() + "\r\n"
					if got := env.buf.String(); got != want {
						t.Fatalf("want only error %q, got %q", want, got)
					}

					env.eng.SetBroadcaster(nil)

					if inTransaction {
						if err := emitStreamInsert(cmd, "after", shared.StreamID{Ms: 1}, nil); err != nil {
							t.Fatal(err)
						}
					}
					// The dispatcher flushes even after a runner writes an error.
					if err := env.ctx.Flush(); err != nil {
						t.Fatal(err)
					}

					snap, _, _, _, err := getStreamSnapshot(cmd, "s")
					if err != nil {
						t.Fatal(err)
					}

					if entries := snapshotToEntries(snap); len(entries) != 1 || entries[0].ID.Ms != 1 {
						t.Fatalf("failed command changed stream: %+v", entries)
					}

					gSnap, _, err := getGroupSnapshot(cmd, "s", "g")
					if err != nil {
						t.Fatal(err)
					}

					last, _, _ := getGroupMeta(gSnap)
					if !last.IsZero() || len(getConsumersFromGroup(gSnap)) != 0 {
						t.Fatalf("failed delivery changed group state: %v", gSnap)
					}

					pel, _, err := getPELSnapshot(cmd, "s", "g")
					if err != nil {
						t.Fatal(err)
					}

					if len(pelToEntries(pel)) != 0 {
						t.Fatal("failed delivery left pending entries")
					}

					if inTransaction {
						for _, key := range []string{"before", "after"} {
							snap, _, _, _, err := getStreamSnapshot(cmd, key)
							if err != nil {
								t.Fatal(err)
							}

							if streamLen(snap) != 1 {
								t.Fatalf("lost surrounding transaction write to %q", key)
							}
						}
					}
				})
			}
		})
	}
}

type failingCloudReader struct{}

func (failingCloudReader) MayHold(string) bool { return true }
func (failingCloudReader) MarkPending(string)  {}
func (failingCloudReader) CloudTips(context.Context, string) ([]effects.Tip, []effects.CloudEffect, []effects.Tip, error) {
	return nil, nil, nil, errors.New("injected cloud read failure")
}

func TestXReadGroupSnapshotFailure(t *testing.T) {
	for _, claim := range []bool{false, true} {
		env := newTestEnv()
		env.eng.SetCloudReader(failingCloudReader{})

		args := []string{"GROUP", "g", "c"}
		if claim {
			args = append(args, "CLAIM", "0")
		}

		args = append(args, "STREAMS", "missing", ">")
		handleXReadGroup(streamTestCommand(env, args...), env.w, nil, &shared.Connection{})

		if got := env.buf.String(); !strings.HasPrefix(got, "-") || !strings.Contains(got, effects.ErrCloudUnavailable.Error()) {
			t.Fatalf("claim=%v: want storage error, got %q", claim, got)
		}
	}
}

func TestClaimErrorPreservesTransaction(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler shared.HandlerFunc
		args    []string
	}{
		{"XCLAIM", handleXClaim, []string{"s", "missing-group", "c", "0", "1-0"}},
		{"XAUTOCLAIM", handleXAutoClaim, []string{"s", "missing-group", "c", "0", "0-0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newTestEnv()
			xadd(t, env, "s", "1-0", "f", "v")
			env.ctx.BeginTx()

			cmd := streamTestCommand(env)
			if err := emitStreamInsert(cmd, "before", shared.StreamID{Ms: 1}, nil); err != nil {
				t.Fatal(err)
			}

			runWithoutFlush(t, env, tc.handler, tc.args...)

			if got := env.buf.String(); !strings.HasPrefix(got, "-NOGROUP ") {
				t.Fatalf("want NOGROUP error, got %q", got)
			}

			if err := env.ctx.Flush(); err != nil {
				t.Fatal(err)
			}

			snap, _, _, _, err := getStreamSnapshot(cmd, "before")
			if err != nil {
				t.Fatal(err)
			}

			if streamLen(snap) != 1 {
				t.Fatal("claim error discarded an earlier transaction write")
			}
		})
	}
}
