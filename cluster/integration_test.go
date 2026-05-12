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
	"fmt"
	"sync"
	"testing"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
)

func TestIntegration_TwoNodeNotification(t *testing.T) {
	handler0 := &recordingHandler{}
	handler1 := &recordingHandler{}
	lr := newStaticLogReader()

	pms := startNNodeCluster(t,
		[]EffectHandler{handler0, handler1},
		[]LogReader{lr, lr},
	)

	pms[0].Broadcast(&pb.OffsetNotify{
		Origin: &pb.EffectRef{NodeId: 0, Offset: 100},
		Key:    []byte("foo"),
	})

	if !handler1.waitForCount(1, 5*time.Second) {
		t.Fatal("node 1 did not receive notification from node 0")
	}

	msgs := handler1.getAll()
	if string(msgs[0].Key) != "foo" {
		t.Fatalf("expected key 'foo', got %q", msgs[0].Key)
	}
	if msgs[0].Origin.Offset != 100 {
		t.Fatalf("unexpected origin: offset=%d", msgs[0].Origin.Offset)
	}
}

func TestIntegration_ThreeNodeFanOut(t *testing.T) {
	handler0 := &recordingHandler{}
	handler1 := &recordingHandler{}
	handler2 := &recordingHandler{}
	lr := newStaticLogReader()

	pms := startNNodeCluster(t,
		[]EffectHandler{handler0, handler1, handler2},
		[]LogReader{lr, lr, lr},
	)

	pms[0].Broadcast(&pb.OffsetNotify{
		Origin: &pb.EffectRef{NodeId: 0, Offset: 500},
		Key:    []byte("fanout-key"),
	})

	if !handler1.waitForCount(1, 3*time.Second) {
		t.Fatal("node 1 did not receive notification")
	}
	if !handler2.waitForCount(1, 3*time.Second) {
		t.Fatal("node 2 did not receive notification")
	}

	for i, h := range []*recordingHandler{handler1, handler2} {
		msgs := h.getAll()
		if string(msgs[0].Key) != "fanout-key" {
			t.Fatalf("node %d: expected key 'fanout-key', got %q", i+1, msgs[0].Key)
		}
	}
}

func TestIntegration_FetchBetweenNodes(t *testing.T) {
	handler0 := &recordingHandler{}
	handler1 := &recordingHandler{}

	lr0 := newStaticLogReader()
	lr1 := newStaticLogReader()

	effectData := []byte("effect-payload-for-fetch")
	lr0.Put(0, 200, effectData)

	pms := startNNodeCluster(t,
		[]EffectHandler{handler0, handler1},
		[]LogReader{lr0, lr1},
	)

	data, err := pms[1].Fetch(&pb.EffectRef{NodeId: 0, Offset: 200})
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if string(data) != string(effectData) {
		t.Fatalf("expected %q, got %q", effectData, data)
	}
}

func TestIntegration_FetchFromAny(t *testing.T) {
	handler0 := &recordingHandler{}
	handler1 := &recordingHandler{}

	lr0 := newStaticLogReader()
	lr1 := newStaticLogReader()

	lr0.Put(0, 999, []byte("found-it"))

	pms := startNNodeCluster(t,
		[]EffectHandler{handler0, handler1},
		[]LogReader{lr0, lr1},
	)

	data, err := pms[1].FetchFromAny(&pb.EffectRef{NodeId: 0, Offset: 999})
	if err != nil {
		t.Fatalf("FetchFromAny failed: %v", err)
	}
	if string(data) != "found-it" {
		t.Fatalf("expected 'found-it', got %q", data)
	}
}

func TestIntegration_BidirectionalBroadcast(t *testing.T) {
	handler0 := &recordingHandler{}
	handler1 := &recordingHandler{}
	lr := newStaticLogReader()

	pms := startNNodeCluster(t,
		[]EffectHandler{handler0, handler1},
		[]LogReader{lr, lr},
	)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		pms[0].Broadcast(&pb.OffsetNotify{
			Origin: &pb.EffectRef{NodeId: 0, Offset: 10},
			Key:    []byte("from-node-0"),
		})
	}()
	go func() {
		defer wg.Done()
		pms[1].Broadcast(&pb.OffsetNotify{
			Origin: &pb.EffectRef{NodeId: 1, Offset: 20},
			Key:    []byte("from-node-1"),
		})
	}()
	wg.Wait()

	if !handler0.waitForCount(1, 3*time.Second) {
		t.Fatal("node 0 did not receive notification from node 1")
	}
	if !handler1.waitForCount(1, 3*time.Second) {
		t.Fatal("node 1 did not receive notification from node 0")
	}

	msgs0 := handler0.getAll()
	if string(msgs0[0].Key) != "from-node-1" {
		t.Fatalf("node 0: expected key 'from-node-1', got %q", msgs0[0].Key)
	}

	msgs1 := handler1.getAll()
	if string(msgs1[0].Key) != "from-node-0" {
		t.Fatalf("node 1: expected key 'from-node-0', got %q", msgs1[0].Key)
	}
}

func TestIntegration_TopologyHotReload(t *testing.T) {
	handler0 := &recordingHandler{}
	handler1 := &recordingHandler{}
	handler2 := &recordingHandler{}
	lr := newStaticLogReader()

	// Start with 2 nodes
	pms := startNNodeCluster(t,
		[]EffectHandler{handler0, handler1},
		[]LogReader{lr, lr},
	)

	clusterTLS := pms[0].serverTLS

	// Start node 2 separately with port 0
	cfg2 := &ClusterConfig{
		NodeID: 2,
		Nodes: []NodeConfig{
			{ID: 2, Address: "127.0.0.1:0", Region: "test"},
		},
	}
	pm2, pmErr := NewPeerManager(cfg2, handler2, lr)
	if pmErr != nil {
		t.Fatalf("failed to create pm2: %v", pmErr)
	}
	pm2.serverTLS = clusterTLS
	pm2.clientTLS = clusterTLS
	pmCtx, pmCancel := context.WithCancel(context.Background())
	if err := pm2.Start(pmCtx); err != nil {
		pmCancel()
		t.Fatalf("start pm2: %v", err)
	}
	t.Cleanup(func() {
		pmCancel()
		pm2.Stop()
	})

	allNodes := []NodeConfig{
		{ID: 0, Address: pms[0].ListenAddr(), Region: "test"},
		{ID: 1, Address: pms[1].ListenAddr(), Region: "test"},
		{ID: 2, Address: pm2.ListenAddr(), Region: "test"},
	}

	// Update topologies to include node 2
	pm2.UpdateTopology(&ClusterConfig{NodeID: 2, Nodes: allNodes})
	pms[0].UpdateTopology(&ClusterConfig{NodeID: 0, Nodes: allNodes})

	// Wait for QUIC connection
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer waitCancel()

	if !pms[0].WaitForPeer(waitCtx, 2) {
		t.Fatal("pm0 did not connect to pm2 after topology update")
	}

	// Wait for heartbeat to establish liveness and symmetry for node 2
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if pms[0].healthTable != nil {
			ph := pms[0].healthTable.GetOrCreate(2)
			if ph.alive.Load() && ph.symmetric.Load() {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Broadcast from node 0 should now reach node 2
	pms[0].Broadcast(&pb.OffsetNotify{
		Origin: &pb.EffectRef{NodeId: 0, Offset: 999},
		Key:    []byte("hot-reload-key"),
	})

	if !handler2.waitForCount(1, 3*time.Second) {
		t.Fatal("node 2 did not receive notification after topology hot-reload")
	}

	msgs := handler2.getAll()
	if string(msgs[0].Key) != "hot-reload-key" {
		t.Fatalf("expected key 'hot-reload-key', got %q", msgs[0].Key)
	}
}

func TestIntegration_MultipleNotifications(t *testing.T) {
	handler0 := &recordingHandler{}
	handler1 := &recordingHandler{}
	lr := newStaticLogReader()

	pms := startNNodeCluster(t,
		[]EffectHandler{handler0, handler1},
		[]LogReader{lr, lr},
	)

	// Send 100 notifications rapidly
	numNotifications := 100
	for i := range numNotifications {
		pms[0].Broadcast(&pb.OffsetNotify{
			Origin: &pb.EffectRef{NodeId: 0, Offset: uint64(i * 100)},
			Key:    fmt.Appendf(nil, "key-%d", i),
		})
	}

	if !handler1.waitForCount(int32(numNotifications), 5*time.Second) {
		t.Fatalf("expected %d notifications, got %d", numNotifications, handler1.count.Load())
	}

	msgs := handler1.getAll()
	if len(msgs) != numNotifications {
		seen := make(map[string]int)
		for _, m := range msgs {
			seen[string(m.Key)]++
		}
		for k, c := range seen {
			if c > 1 {
				t.Logf("duplicate key %q appeared %d times", k, c)
			}
		}
		t.Fatalf("expected %d messages, got %d", numNotifications, len(msgs))
	}
}

func TestIntegration_NotificationWithDeps(t *testing.T) {
	handler0 := &recordingHandler{}
	handler1 := &recordingHandler{}
	lr := newStaticLogReader()

	pms := startNNodeCluster(t,
		[]EffectHandler{handler0, handler1},
		[]LogReader{lr, lr},
	)

	pms[0].Broadcast(&pb.OffsetNotify{
		Origin: &pb.EffectRef{NodeId: 0, Offset: 500},
		Key:    []byte("chained-key"),
		Deps: []*pb.EffectRef{
			{NodeId: 0, Offset: 300},
			{NodeId: 1, Offset: 200},
		},
	})

	if !handler1.waitForCount(1, 3*time.Second) {
		t.Fatal("node 1 did not receive notification with deps")
	}

	msgs := handler1.getAll()
	if len(msgs[0].Deps) != 2 {
		t.Fatalf("expected 2 deps, got %d", len(msgs[0].Deps))
	}
	if msgs[0].Deps[0].Offset != 300 || msgs[0].Deps[1].Offset != 200 {
		t.Fatalf("unexpected dep offsets: %+v", msgs[0].Deps)
	}
}
