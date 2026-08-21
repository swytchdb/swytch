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

package effects

import (
	"fmt"
	"slices"
	"testing"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNormalizeCloudTipCandidatesDropsMissingStaleMarker(t *testing.T) {
	stale := Tip{111, 1}
	realTip := Tip{222, 2}
	realEff := &pb.Effect{Deps: []*pb.EffectRef{toPbRef(stale)}}

	real, superseded, err := NormalizeCloudTipCandidates("k", []Tip{stale, realTip}, nil, func(tip Tip) (*pb.Effect, error) {
		if tip == realTip {
			return realEff, nil
		}
		return nil, fmt.Errorf("fetch %v: %w", tip, ErrCDNBlobMissing)
	})
	if err != nil {
		t.Fatalf("normalize candidates: %v", err)
	}
	if !slices.Equal(real, []Tip{realTip}) {
		t.Fatalf("real tips = %v, want %v", real, []Tip{realTip})
	}
	if !slices.Equal(superseded, []Tip{stale}) {
		t.Fatalf("superseded candidates = %v, want %v", superseded, []Tip{stale})
	}
}

// TestNormalizeCloudTipCandidatesDropsMarkerCoveredByLocalAncestry: a
// candidate already superseded by this node's own local tips — not yet
// reflected in Cloud's index — must drop as superseded too, the same as if
// another candidate had covered it. Local roots seed the walk but never
// classify as superseded themselves: they aren't entries in Cloud's index.
func TestNormalizeCloudTipCandidatesDropsMarkerCoveredByLocalAncestry(t *testing.T) {
	stale := Tip{111, 1}
	localTip := Tip{222, 2}
	localEff := &pb.Effect{Deps: []*pb.EffectRef{toPbRef(stale)}}

	real, superseded, err := NormalizeCloudTipCandidates("k", []Tip{stale}, []Tip{localTip}, func(tip Tip) (*pb.Effect, error) {
		if tip == localTip {
			return localEff, nil
		}
		return nil, fmt.Errorf("fetch %v: %w", tip, ErrCDNBlobMissing)
	})
	if err != nil {
		t.Fatalf("normalize candidates: %v", err)
	}
	if len(real) != 0 {
		t.Fatalf("real tips = %v, want none: the only candidate is covered by local ancestry", real)
	}
	if !slices.Equal(superseded, []Tip{stale}) {
		t.Fatalf("superseded candidates = %v, want %v", superseded, []Tip{stale})
	}
}

// holedFetchBroadcaster answers every fetch with the CDN-404 sentinel: any
// offset not already in the engine's cache is a provable durable hole.
type holedFetchBroadcaster struct {
	mockBroadcaster
}

func (b *holedFetchBroadcaster) FetchFromAny(ref *pb.EffectRef, _ FetchHint) ([]byte, error) {
	return nil, fmt.Errorf("cdn fetch %v: %w", r(ref), ErrCDNBlobMissing)
}

// TestInstallCloudTipsNeverMints: a cloud frontier tip whose walk fails with
// the CDN-404 sentinel must not be repaired with a synthesized snapshot —
// InstallCloudTips never mints and never touches local ancestry. The holed
// tip is reported back as pending for the caller to hand to Cloud's reconcile
// loop, and the engine's own local state is left exactly as it was.
func TestInstallCloudTipsNeverMints(t *testing.T) {
	e := newTxnTestEngine(&holedFetchBroadcaster{})
	defer func() { _ = e.Close() }()

	ctx := e.NewContext()
	if err := ctx.Emit(scalarSet("k", "local")); err != nil {
		t.Fatal(err)
	}
	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}
	before := e.index.Contains("k").Tips()

	holedTip := Tip{12345, 7}
	pending, err := e.InstallCloudTips("k", []Tip{holedTip}, nil, nil)
	if err != nil {
		t.Fatalf("install over holed frontier: %v", err)
	}
	if !slices.Equal(pending, []Tip{holedTip}) {
		t.Fatalf("pending = %v, want [%v]", pending, holedTip)
	}

	after := e.index.Contains("k").Tips()
	if !slices.Equal(before, after) {
		t.Fatalf("local tips changed from %v to %v: install must never mint over a hole", before, after)
	}

	result, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatalf("post-install read: %v", err)
	}
	if result == nil || result.Scalar == nil || string(result.Scalar.GetRaw()) != "local" {
		t.Fatalf("post-install state = %v, want scalar %q", result, "local")
	}
}

// TestInstallCloudTipsPreservesReadableSiblings: when only part of Cloud's
// frontier sits above a hole, the readable tip must still install and serve
// on its own — a hole in one candidate is not grounds to withhold the rest of
// the frontier — while the holed tip is reported pending instead of blocking
// the install.
func TestInstallCloudTipsPreservesReadableSiblings(t *testing.T) {
	e := newTxnTestEngine(&holedFetchBroadcaster{})
	defer func() { _ = e.Close() }()

	hlc := timestamppb.New(time.Now())
	readableTip := Tip{1111, 1}
	readableEff := &pb.Effect{
		Key:            []byte("k"),
		Hlc:            hlc,
		ForkChoiceHash: ComputeForkChoiceHash(1111, hlc),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: []byte("cloud")},
		}},
	}
	holedTip := Tip{12345, 7}

	sidecar := []CloudEffect{{Tip: readableTip, Eff: readableEff, ProtoLen: proto.Size(readableEff)}}
	pending, err := e.InstallCloudTips("k", []Tip{readableTip, holedTip}, sidecar, nil)
	if err != nil {
		t.Fatalf("install over partially holed frontier: %v", err)
	}
	if !slices.Equal(pending, []Tip{holedTip}) {
		t.Fatalf("pending = %v, want [%v]", pending, holedTip)
	}

	ts := e.index.Contains("k")
	if ts == nil || !slices.Contains(ts.Tips(), readableTip) {
		t.Fatalf("post-install tips = %v, want the readable tip installed", ts.Tips())
	}
	if ts != nil && slices.Contains(ts.Tips(), holedTip) {
		t.Fatalf("post-install tips = %v, holed tip must never be installed", ts.Tips())
	}

	result, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatalf("post-install read: %v", err)
	}
	if result == nil || result.Scalar == nil || string(result.Scalar.GetRaw()) != "cloud" {
		t.Fatalf("post-install state = %v, want the readable sibling's scalar %q", result, "cloud")
	}
}
