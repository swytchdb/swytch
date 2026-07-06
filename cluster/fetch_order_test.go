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
	"context"
	"errors"
	"sync"
	"testing"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
)

// recordingCDN is a CDNFetcher fake that counts calls and serves fixed data
// or a fixed error.
type recordingCDN struct {
	mu    sync.Mutex
	calls int
	data  []byte
	err   error
}

func (c *recordingCDN) FetchFromCDN(context.Context, *pb.EffectRef) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return c.data, c.err
}

func (c *recordingCDN) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// newFetchTestPM builds an unstarted PeerManager with zero peers: every peer
// leg fails immediately with ErrPeerUnavailable, isolating the hint-ordered
// fallback logic.
func newFetchTestPM(t *testing.T) *PeerManager {
	t.Helper()
	cfg := &ClusterConfig{
		NodeID: 0,
		Nodes:  []NodeConfig{{ID: 0, Address: "127.0.0.1:0", Region: "test"}},
	}
	pm, err := NewPeerManager(cfg, &recordingHandler{}, &mockLogReader{})
	if err != nil {
		t.Fatalf("NewPeerManager: %v", err)
	}
	pm.ctx = t.Context()
	return pm
}

func TestFetchFromAnyPreferPeersFallsBackToCDN(t *testing.T) {
	pm := newFetchTestPM(t)
	cdn := &recordingCDN{data: []byte("cdn-bytes")}
	pm.SetCDNFetcher(cdn)

	data, err := pm.FetchFromAny(&pb.EffectRef{NodeId: 1, Offset: 1}, effects.PreferPeers)
	if err != nil {
		t.Fatalf("expected CDN fallback to serve the fetch, got error: %v", err)
	}
	if string(data) != "cdn-bytes" {
		t.Fatalf("got %q, want the CDN bytes", data)
	}
	if cdn.callCount() != 1 {
		t.Fatalf("CDN called %d times, want exactly 1", cdn.callCount())
	}
}

func TestFetchFromAnyPreferCDNServesWithoutPeers(t *testing.T) {
	pm := newFetchTestPM(t)
	cdn := &recordingCDN{data: []byte("cdn-bytes")}
	pm.SetCDNFetcher(cdn)

	data, err := pm.FetchFromAny(&pb.EffectRef{NodeId: 1, Offset: 1}, effects.PreferCDN)
	if err != nil {
		t.Fatalf("PreferCDN fetch errored: %v", err)
	}
	if string(data) != "cdn-bytes" {
		t.Fatalf("got %q, want the CDN bytes", data)
	}
	if cdn.callCount() != 1 {
		t.Fatalf("CDN called %d times, want exactly 1", cdn.callCount())
	}
}

func TestFetchFromAnyPreferCDNFallsBackToPeers(t *testing.T) {
	pm := newFetchTestPM(t)
	cdn := &recordingCDN{err: errors.New("edge reset the stream")}
	pm.SetCDNFetcher(cdn)

	_, err := pm.FetchFromAny(&pb.EffectRef{NodeId: 1, Offset: 1}, effects.PreferCDN)
	if err == nil {
		t.Fatal("expected an error when both CDN and peers fail")
	}
	if cdn.callCount() != 1 {
		t.Fatalf("CDN called %d times, want exactly 1 (no retry loop)", cdn.callCount())
	}
}

func TestFetchFromAnyNoCDNFetcherPeersOnly(t *testing.T) {
	pm := newFetchTestPM(t)

	_, err := pm.FetchFromAny(&pb.EffectRef{NodeId: 1, Offset: 1}, effects.PreferCDN)
	if !errors.Is(err, ErrPeerUnavailable) {
		t.Fatalf("want ErrPeerUnavailable with no CDN fetcher and no peers, got: %v", err)
	}
}
