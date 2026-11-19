package redis

import (
	"errors"
	"sync/atomic"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

type ambiguousCommandPeer struct {
	effects.Broadcaster

	binds atomic.Int32
}

func (*ambiguousCommandPeer) PeerIDs() []pb.NodeID                       { return []pb.NodeID{2} }
func (*ambiguousCommandPeer) InMajorityPartition() bool                  { return true }
func (*ambiguousCommandPeer) AllRegionPeersReachable() bool              { return true }
func (*ambiguousCommandPeer) Broadcast(*pb.OffsetNotify)                 {}
func (*ambiguousCommandPeer) BroadcastWithData(*pb.OffsetNotify, []byte) {}
func (*ambiguousCommandPeer) Replicate(*pb.OffsetNotify, []byte) error   { return nil }
func (p *ambiguousCommandPeer) ReplicateMarshalled(*pb.OffsetNotify, []byte, pb.NodeID) ([]*pb.NackNotify, error) {
	p.binds.Add(1)

	return nil, errors.New("ACK unavailable")
}

func ambiguousHandler(t *testing.T) (*Handler, *ambiguousCommandPeer) {
	t.Helper()
	h := newSSIHandler(t)

	run := commandClient(h)
	if reply := run("SET", "count", "0"); reply != "+OK\r\n" {
		t.Fatal(reply)
	}

	ctx := h.engine.NewContext()
	if err := ctx.Emit(&pb.Effect{
		Key:  []byte("count"),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 2}},
	}); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	peer := &ambiguousCommandPeer{}
	h.engine.SetBroadcaster(peer)

	return h, peer
}

func TestAmbiguousCommitResponse(t *testing.T) {
	for _, mode := range []string{"ssi", "queued", "implicit", "forwarded"} {
		t.Run(mode, func(t *testing.T) {
			h, peer := ambiguousHandler(t)
			c := ssiConn(h)

			var got string

			switch mode {
			case "ssi", "queued":
				h.ssi = mode == "ssi"
				ssiWant(t, h, c, "+OK\r\n", "MULTI")

				want := ":1\r\n"
				if mode == "queued" {
					want = "+QUEUED\r\n"
				}

				ssiWant(t, h, c, want, "INCR", "count")
				got = ssiCommand(h, c, "EXEC")
			case "implicit":
				got = ssiCommand(h, c, "GETSET", "count", "1")
			case "forwarded":
				resp := h.HandleForwardedTransaction(forwardedCommands(
					[]string{"INCR", "count"}, []string{"GET", "count"},
				))
				if resp.GetError() {
					t.Fatalf("error code lost in forwarding envelope: %v", resp)
				}

				got = string(resp.GetRespData())
			}

			if want := "-AMBIGUOUS transaction outcome unknown; commit may have occurred\r\n"; got != want {
				t.Fatalf("got %q, want %q", got, want)
			}

			if peer.binds.Load() != 1 {
				t.Fatalf("ambiguous operation was retried: %d binds", peer.binds.Load())
			}

			if c.InTransaction {
				t.Fatal("transaction state leaked after ambiguous EXEC")
			}

			ssiWant(t, h, c, "+PONG\r\n", "PING")
		})
	}
}

func TestAmbiguousCommitRESP3(t *testing.T) {
	h, peer := ambiguousHandler(t)
	c := ssiConn(h)
	c.Protocol = shared.RESP3
	ssiWant(t, h, c, "+OK\r\n", "MULTI")
	ssiWant(t, h, c, ":1\r\n", "INCR", "count")
	ssiWant(t, h, c, "-AMBIGUOUS transaction outcome unknown; commit may have occurred\r\n", "EXEC")

	if peer.binds.Load() != 1 {
		t.Fatalf("ambiguous transaction was retried: %d binds", peer.binds.Load())
	}
}
