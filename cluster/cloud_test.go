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
	"bytes"
	"testing"

	pb "github.com/swytchdb/swytch/cluster/proto"
	clpb "github.com/swytchdb/swytch/cluster/proto/cloud"
	"github.com/swytchdb/swytch/effects"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newTestCloudClient(t *testing.T) *CloudClient {
	t.Helper()
	conn, err := GenerateConnectionSecret()
	if err != nil {
		t.Fatalf("generate connection secret: %v", err)
	}
	c, err := NewCloudClient(conn, nil)
	if err != nil {
		t.Fatalf("new cloud client: %v", err)
	}
	return c
}

// jobFor frames an effect as the sender would receive it off the broadcast tee.
func jobFor(t *testing.T, eff *pb.Effect) cloudJob {
	t.Helper()
	data, err := effects.MarshalEffect(eff)
	if err != nil {
		t.Fatalf("marshal effect: %v", err)
	}
	return cloudJob{
		ref:  effects.Tip{eff.NodeId, 42},
		wire: buildWireFrame(eff.Key, data),
	}
}

// TestBuildCloudEffectRoundTrip is the core write-path property: an effect maps
// to an encrypted cloud effect whose envelope the cloud can read (structural
// fields, deterministic key) but whose payload only the customer key can open,
// decrypting back to the original effect.
func TestBuildCloudEffectRoundTrip(t *testing.T) {
	c := newTestCloudClient(t)

	eff := &pb.Effect{
		Key:    []byte("__swytch:members"),
		Hlc:    timestamppb.New(timestamppb.Now().AsTime()),
		NodeId: 7,
		Deps:   []*pb.EffectRef{{NodeId: 7, Offset: 41}, {NodeId: 9, Offset: 3}},
		Kind:   &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP}},
	}

	out, err := c.buildCloudEffect(jobFor(t, eff))
	if err != nil {
		t.Fatalf("buildCloudEffect: %v", err)
	}
	if out == nil {
		t.Fatal("data effect was filtered out")
	}

	// Structural envelope stays in the clear for the cloud.
	if out.GetId().GetNodeId() != 7 || out.GetId().GetOffset() != 42 {
		t.Fatalf("id mismatch: %v", out.GetId())
	}
	if out.GetEffectType() != clpb.EffectType_EFFECT_TYPE_DATA {
		t.Fatalf("effect type: got %v want DATA", out.GetEffectType())
	}
	if len(out.GetDeps()) != 2 || out.GetDeps()[1].GetNodeId() != 9 {
		t.Fatalf("deps not carried through: %v", out.GetDeps())
	}

	// Key name is encrypted but recoverable with the customer key.
	if bytes.Equal(out.GetKey(), eff.Key) {
		t.Fatal("key name shipped in the clear")
	}
	name, err := c.crypto.openKeyName(out.GetKey())
	if err != nil {
		t.Fatalf("open key name: %v", err)
	}
	if !bytes.Equal(name, eff.Key) {
		t.Fatalf("key name round-trip: got %q want %q", name, eff.Key)
	}

	// Payload is opaque to the cloud but decrypts to the original effect.
	if bytes.Contains(out.GetRawEffect(), eff.Key) {
		t.Fatal("payload leaked plaintext key")
	}
	protoData, err := c.crypto.openPayload(out.GetRawEffect())
	if err != nil {
		t.Fatalf("open payload: %v", err)
	}
	got := &pb.Effect{}
	if err := effects.UnmarshalEffect(protoData, got); err != nil {
		t.Fatalf("unmarshal decrypted effect: %v", err)
	}
	if got.GetData().GetOp() != pb.EffectOp_INSERT_OP || got.NodeId != 7 {
		t.Fatalf("decrypted effect mismatch: %+v", got)
	}
}

// TestBuildCloudEffectFilters confirms the effects that must never be durabilized
// are dropped (nil, nil) rather than uploaded.
func TestBuildCloudEffectFilters(t *testing.T) {
	c := newTestCloudClient(t)

	pubsub := &pb.Effect{
		Key:  []byte("chan"),
		Kind: &pb.Effect_PubsubMessage{PubsubMessage: &pb.PubSubMessage{Channel: []byte("chan")}},
	}
	if out, err := c.buildCloudEffect(jobFor(t, pubsub)); err != nil || out != nil {
		t.Fatalf("pubsub message not filtered: out=%v err=%v", out, err)
	}

	ephemeral := &pb.Effect{
		Key: []byte("k"),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			SubscriberNodeId: 1,
			Ephemeral:        true,
		}},
	}
	if out, err := c.buildCloudEffect(jobFor(t, ephemeral)); err != nil || out != nil {
		t.Fatalf("ephemeral subscription not filtered: out=%v err=%v", out, err)
	}
}

// TestCloudTeeSkipsSynthetic confirms a synthetic (node 0) probe never enqueues.
func TestCloudTeeSkipsSynthetic(t *testing.T) {
	c := newTestCloudClient(t)
	c.tee(&pb.OffsetNotify{Origin: &pb.EffectRef{NodeId: 0, Offset: 5}, EffectData: []byte("x")})
	select {
	case <-c.queue:
		t.Fatal("synthetic node-0 effect was queued")
	default:
	}
}
