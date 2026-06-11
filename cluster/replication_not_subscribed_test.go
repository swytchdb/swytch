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

package cluster

import (
	"errors"
	"testing"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
)

// notSubscribedHandler always NACKs with NotSubscribed, simulating a peer
// that has no authority on the key (the exact path HandleRemote takes at
// effects/effect.go:817-819).
type notSubscribedHandler struct{}

func (h *notSubscribedHandler) HandleOffsetNotify(notify *pb.OffsetNotify) ([]*pb.NackNotify, error) {
	return []*pb.NackNotify{{Key: notify.Key, NotSubscribed: true}}, nil
}

func (h *notSubscribedHandler) HandleNack(_ *pb.NackNotify) error {
	return nil
}

// TestReplicate_AllPeersNotSubscribed_FastReject verifies that when every
// peer NACKs with NotSubscribed, the Replicate future rejects immediately
// with ErrAllPeersNotSubscribed rather than waiting the full 30s timeout.
//
// This reproduces the production stall: a SafeMode SET on a key no peer is
// subscribed to blocks the redis command handler for the entire replication
// timeout because the fast-reject path doesn't fire.
func TestReplicate_AllPeersNotSubscribed_FastReject(t *testing.T) {
	handlers := []EffectHandler{
		&recordingHandler{},     // node 0: originator (we call Replicate from here)
		&notSubscribedHandler{}, // node 1: always NotSubscribed
		&notSubscribedHandler{}, // node 2: always NotSubscribed
	}
	logReaders := []LogReader{
		&mockLogReader{data: []byte("test")},
		&mockLogReader{data: []byte("test")},
		&mockLogReader{data: []byte("test")},
	}

	pms := startNNodeCluster(t, handlers, logReaders)

	notify := &pb.OffsetNotify{
		Origin: &pb.EffectRef{NodeId: uint64(pms[0].config.NodeID), Offset: 1},
		Key:    []byte("test-key"),
	}
	wireData := []byte("fake-effect-data")

	start := time.Now()
	err := pms[0].Replicate(notify, wireData)
	elapsed := time.Since(start)

	if elapsed > 5*time.Second {
		t.Fatalf("Replicate took %v — expected fast reject, got timeout", elapsed)
	}

	if !errors.Is(err, ErrAllPeersNotSubscribed) {
		t.Fatalf("expected ErrAllPeersNotSubscribed, got: %v", err)
	}

	t.Logf("Replicate rejected in %v with: %v", elapsed, err)
}

// TestReplicate_AllPeersNotSubscribed_BroadcastThenReplicate reproduces the
// exact SafeMode flush pattern: BroadcastWithData (fire-and-forget) for
// intermediate effects, then Replicate (blocking) for the final one. Both
// go to peers that NACKs with NotSubscribed. The Replicate must still
// fast-reject and not be confused by the BroadcastWithData tracked requests.
func TestReplicate_AllPeersNotSubscribed_BroadcastThenReplicate(t *testing.T) {
	handlers := []EffectHandler{
		&recordingHandler{},
		&notSubscribedHandler{},
		&notSubscribedHandler{},
	}
	logReaders := []LogReader{
		&mockLogReader{data: []byte("test")},
		&mockLogReader{data: []byte("test")},
		&mockLogReader{data: []byte("test")},
	}

	pms := startNNodeCluster(t, handlers, logReaders)

	// Intermediate effect — fire and forget (like BroadcastWithData)
	intermediate := &pb.OffsetNotify{
		Origin: &pb.EffectRef{NodeId: uint64(pms[0].config.NodeID), Offset: 1},
		Key:    []byte("test-key"),
	}
	pms[0].BroadcastWithData(intermediate, []byte("intermediate-data"))

	// Small delay so intermediate NACKs are in flight
	time.Sleep(10 * time.Millisecond)

	// Final effect — blocking (like SafeMode Replicate)
	final := &pb.OffsetNotify{
		Origin: &pb.EffectRef{NodeId: uint64(pms[0].config.NodeID), Offset: 2},
		Key:    []byte("test-key"),
	}

	start := time.Now()
	err := pms[0].Replicate(final, []byte("final-data"))
	elapsed := time.Since(start)

	if elapsed > 5*time.Second {
		t.Fatalf("Replicate took %v after BroadcastWithData — expected fast reject", elapsed)
	}

	if !errors.Is(err, ErrAllPeersNotSubscribed) {
		t.Fatalf("expected ErrAllPeersNotSubscribed, got: %v", err)
	}

	t.Logf("BroadcastWithData+Replicate: rejected in %v", elapsed)
}

// TestReplicate_AllPeersNotSubscribed_HighConcurrency floods the system
// with many concurrent BroadcastWithData + Replicate pairs, simulating the
// production workload where hundreds of keys are being written simultaneously
// and peers NACK everything with NotSubscribed.
func TestReplicate_AllPeersNotSubscribed_HighConcurrency(t *testing.T) {
	handlers := []EffectHandler{
		&recordingHandler{},
		&notSubscribedHandler{},
		&notSubscribedHandler{},
	}
	logReaders := []LogReader{
		&mockLogReader{data: []byte("test")},
		&mockLogReader{data: []byte("test")},
		&mockLogReader{data: []byte("test")},
	}

	pms := startNNodeCluster(t, handlers, logReaders)

	const numOps = 200
	errs := make(chan error, numOps)
	durations := make(chan time.Duration, numOps)

	for i := range numOps {
		go func(i int) {
			origin := uint64(pms[0].config.NodeID)

			// Intermediate effect (fire-and-forget)
			intermediate := &pb.OffsetNotify{
				Origin: &pb.EffectRef{NodeId: origin, Offset: uint64(i*2 + 1)},
				Key:    []byte("key-" + string(rune('A'+i%26))),
			}
			pms[0].BroadcastWithData(intermediate, []byte("intermediate"))

			// Final effect (blocking)
			final := &pb.OffsetNotify{
				Origin: &pb.EffectRef{NodeId: origin, Offset: uint64(i*2 + 2)},
				Key:    []byte("key-" + string(rune('A'+i%26))),
			}
			start := time.Now()
			err := pms[0].Replicate(final, []byte("final"))
			durations <- time.Since(start)
			errs <- err
		}(i)
	}

	var maxDuration time.Duration
	for range numOps {
		d := <-durations
		err := <-errs
		if !errors.Is(err, ErrAllPeersNotSubscribed) {
			t.Errorf("expected ErrAllPeersNotSubscribed, got: %v", err)
		}
		if d > maxDuration {
			maxDuration = d
		}
	}

	if maxDuration > 5*time.Second {
		t.Fatalf("worst-case Replicate took %v — expected fast reject under load", maxDuration)
	}
	t.Logf("200 concurrent ops: worst-case %v", maxDuration)
}
