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
)

func TestMarshalParseHeartbeat(t *testing.T) {
	packet := MarshalHeartbeat(42, 123456789)

	if len(packet) != heartbeatPacketSize {
		t.Fatalf("packet size %d, expected %d", len(packet), heartbeatPacketSize)
	}

	if packet[0] != PacketTypeHeartbeat {
		t.Fatalf("packet type %d, expected %d", packet[0], PacketTypeHeartbeat)
	}

	nodeID, timestamp, err := ParseHeartbeat(packet)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nodeID != 42 {
		t.Fatalf("nodeID %d, expected 42", nodeID)
	}
	if timestamp != 123456789 {
		t.Fatalf("timestamp %d, expected 123456789", timestamp)
	}

	// Test short input
	_, _, err = ParseHeartbeat([]byte{0x01, 0x02})
	if err == nil {
		t.Fatal("expected error for short input")
	}
}

func TestHeartbeatMarksSymmetric(t *testing.T) {
	healthA := NewPeerHealthTable()
	healthB := NewPeerHealthTable()

	hmA := NewHeartbeatManager(1, healthA)
	hmB := NewHeartbeatManager(2, healthB)

	hmA.AddPeer(2, "local")
	hmB.AddPeer(1, "local")

	// A sends heartbeat to B
	ts := uint64(time.Now().UnixNano())
	packetAtoB := MarshalHeartbeat(1, ts)

	// B receives A's heartbeat
	nodeID, timestamp, err := ParseHeartbeat(packetAtoB)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hmB.ProcessInbound(nodeID, timestamp)

	// B should now see A as alive and symmetric
	phB := healthB.Get(1)
	if phB == nil {
		t.Fatal("B should have health entry for A")
	}
	if !phB.alive.Load() {
		t.Fatal("B should see A as alive")
	}
	if !phB.symmetric.Load() {
		t.Fatal("B should see A as symmetric after receiving heartbeat")
	}

	// B sends heartbeat to A
	packetBtoA := MarshalHeartbeat(2, ts+1)

	// A receives B's heartbeat
	nodeID, timestamp, err = ParseHeartbeat(packetBtoA)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hmA.ProcessInbound(nodeID, timestamp)

	// A should now see B as alive and symmetric
	phA := healthA.Get(2)
	if phA == nil {
		t.Fatal("A should have health entry for B")
	}
	if !phA.alive.Load() {
		t.Fatal("A should see B as alive")
	}
	if !phA.symmetric.Load() {
		t.Fatal("A should see B as symmetric after receiving heartbeat")
	}
}

func TestLivenessTimeout(t *testing.T) {
	health := NewPeerHealthTable()
	ph := health.GetOrCreate(1)

	ph.lastHeartbeat.Store(time.Now().Add(-4 * time.Second).UnixNano())
	ph.alive.Store(true)

	health.CheckLiveness(3 * time.Second)

	if ph.alive.Load() {
		t.Fatal("peer should be marked dead after timeout")
	}
}

func TestPeerHealthTable(t *testing.T) {
	table := NewPeerHealthTable()

	ph := table.GetOrCreate(1)
	if ph == nil {
		t.Fatal("should create health entry")
	}

	ph2 := table.Get(1)
	if ph2 != ph {
		t.Fatal("should return same entry")
	}

	if table.Get(99) != nil {
		t.Fatal("should return nil for unknown peer")
	}

	// AliveSymmetricPeers
	ph.alive.Store(true)
	ph.symmetric.Store(true)

	ph3 := table.GetOrCreate(2)
	ph3.alive.Store(true)
	ph3.symmetric.Store(false) // alive but not symmetric

	regions := map[NodeId]string{1: "us-east", 2: "us-east"}
	peers := table.AliveSymmetricPeers("us-east", regions)
	if len(peers) != 1 || peers[0] != 1 {
		t.Fatalf("expected [1], got %v", peers)
	}

	count := table.AliveSymmetricCount("us-east", regions)
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}

	// Remove
	table.Remove(1)
	if table.Get(1) != nil {
		t.Fatal("should be removed")
	}
}
