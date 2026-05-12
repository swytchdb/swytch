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
	"testing"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
)

func TestPeerManagerStartStop(t *testing.T) {
	cfg := &ClusterConfig{
		NodeID: 0,
		Nodes: []NodeConfig{
			{ID: 0, Address: "127.0.0.1:0", Region: "test"},
		},
	}

	handler := &recordingHandler{}
	logReader := &mockLogReader{data: []byte("test-effect")}
	pm, err := NewPeerManager(cfg, handler, logReader)
	if err != nil {
		t.Fatalf("failed to create peer manager: %v", err)
	}

	tlsCfg := generateTestTLSConfig(t)
	pm.serverTLS = tlsCfg
	pm.clientTLS = tlsCfg

	if err := pm.Start(t.Context()); err != nil {
		t.Fatalf("failed to start peer manager: %v", err)
	}

	pm.Stop()
}

func TestPeerManagerBroadcastAndReceive(t *testing.T) {
	handler0 := &recordingHandler{}
	handler1 := &recordingHandler{}
	lr := &mockLogReader{data: []byte("test-effect")}

	pms := startNNodeCluster(t,
		[]EffectHandler{handler0, handler1},
		[]LogReader{lr, lr},
	)

	pms[0].Broadcast(&pb.OffsetNotify{
		Origin: &pb.EffectRef{NodeId: 0, Offset: 100},
		Key:    []byte("test-key"),
	})

	if !handler1.waitForCount(1, 5*time.Second) {
		t.Fatal("node 1 did not receive notification from node 0")
	}
}

func TestPeerManagerFetch(t *testing.T) {
	lr := &mockLogReader{data: []byte("fetched-effect-data")}
	handler := &recordingHandler{}

	pms := startNNodeCluster(t,
		[]EffectHandler{handler, handler},
		[]LogReader{lr, lr},
	)

	data, err := pms[1].Fetch(&pb.EffectRef{NodeId: 0, Offset: 1000})
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}

	if string(data) != "fetched-effect-data" {
		t.Fatalf("expected 'fetched-effect-data', got %q", data)
	}
}

func TestPeerManagerFetchUnavailablePeer(t *testing.T) {
	cfg := &ClusterConfig{
		NodeID: 0,
		Nodes: []NodeConfig{
			{ID: 0, Address: "127.0.0.1:0", Region: "test"},
		},
	}

	pm, err := NewPeerManager(cfg, &recordingHandler{}, &mockLogReader{})
	if err != nil {
		t.Fatalf("failed to create peer manager: %v", err)
	}

	tlsCfg := generateTestTLSConfig(t)
	pm.serverTLS = tlsCfg
	pm.clientTLS = tlsCfg

	if err := pm.Start(t.Context()); err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	defer pm.Stop()

	_, fetchErr := pm.Fetch(&pb.EffectRef{NodeId: 99, Offset: 2})
	if fetchErr != ErrPeerUnavailable {
		t.Fatalf("expected ErrPeerUnavailable, got %v", fetchErr)
	}
}

func TestPeerManagerUpdateTopology(t *testing.T) {
	handler := &recordingHandler{}
	logReader := &mockLogReader{data: []byte("test")}

	cfg := &ClusterConfig{
		NodeID: 0,
		Nodes: []NodeConfig{
			{ID: 0, Address: "127.0.0.1:0", Region: "test"},
			{ID: 1, Address: "127.0.0.1:12345", Region: "test"},
		},
	}

	pm, err := NewPeerManager(cfg, handler, logReader)
	if err != nil {
		t.Fatalf("failed to create peer manager: %v", err)
	}

	tlsCfg := generateTestTLSConfig(t)
	pm.serverTLS = tlsCfg
	pm.clientTLS = tlsCfg

	if err := pm.Start(t.Context()); err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	defer pm.Stop()

	if peerCount := len(pm.PeerIDs()); peerCount != 1 {
		t.Fatalf("expected 1 peer, got %d", peerCount)
	}

	// Add a new peer
	newCfg := &ClusterConfig{
		NodeID: 0,
		Nodes: []NodeConfig{
			{ID: 0, Address: pm.ListenAddr(), Region: "test"},
			{ID: 1, Address: "127.0.0.1:12345", Region: "test"},
			{ID: 2, Address: "127.0.0.1:12346", Region: "test"},
		},
	}
	pm.UpdateTopology(newCfg)

	if peerCount := len(pm.PeerIDs()); peerCount != 2 {
		t.Fatalf("expected 2 peers after add, got %d", peerCount)
	}

	// Remove a peer
	removeCfg := &ClusterConfig{
		NodeID: 0,
		Nodes: []NodeConfig{
			{ID: 0, Address: pm.ListenAddr(), Region: "test"},
			{ID: 2, Address: "127.0.0.1:12346", Region: "test"},
		},
	}
	pm.UpdateTopology(removeCfg)

	ids := pm.PeerIDs()
	if len(ids) != 1 {
		t.Fatalf("expected 1 peer after remove, got %d", len(ids))
	}
	if ids[0] != 2 {
		t.Fatal("expected peer 2 to remain")
	}
}
