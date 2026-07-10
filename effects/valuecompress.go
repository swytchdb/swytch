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
	pb "github.com/swytchdb/swytch/cluster/proto"
)

// Value compression, write side: a writer under --compress stores DataEffect
// values as the CompressedValue arm of the value oneof, so the compressed
// form is what lives in the resident DAG, in reduced state, on the wire, and
// in the cloud. The arm travels with the effect, so reading never depends on
// the writer's settings: every byte-value read goes through
// pb.DataEffect.Decompress(), which hands back the raw arm as-is and
// inflates the compressed arm on demand. Compression is applied per value
// and only kept when it wins (store-if-smaller), so incompressible payloads
// ride the raw arm and mixed clusters share one DAG without ceremony.

// compressMinBytes is the floor below which compression isn't attempted:
// small values are dominated by codec framing, and the byte mass of a
// workload lives in the large ones.
const compressMinBytes = 64

// compressDataValue swaps d's raw arm for a CompressedValue arm when it pays:
// at least compressMinBytes and the codec output strictly smaller. d must be
// exclusively owned by the caller (Emit-time effects are; cached ones never
// are). Non-raw arms and losing trades are left untouched.
func compressDataValue(d *pb.DataEffect) {
	if d == nil {
		return
	}
	v, ok := d.Value.(*pb.DataEffect_Raw)
	if !ok || len(v.Raw) < compressMinBytes {
		return
	}
	z := Compress(v.Raw)
	if len(z) >= len(v.Raw) {
		return
	}
	d.Value = &pb.DataEffect_Compressed{Compressed: &pb.CompressedValue{
		Codec:  pb.Compression_COMPRESSION_ZSTD,
		RawLen: uint64(len(v.Raw)),
		Data:   z,
	}}
}

// CompressEffectValues applies value compression to an effect about to be
// emitted: the data arm directly, and a snapshot's materialized state via
// copy — snapshot state may share element pointers with cached reduced
// state, which is immutable by contract, so compression swaps in fresh
// structs rather than writing through shared ones.
func CompressEffectValues(eff *pb.Effect) {
	if data := eff.GetData(); data != nil {
		compressDataValue(data)
		return
	}
	if snap := eff.GetSnapshot(); snap != nil && snap.State != nil {
		snap.State = compressReducedState(snap.State)
	}
}

// compressReducedState returns r with every compressible value compressed,
// copying only what changes: an untouched state comes back as-is, and a
// touched one is a fresh ReducedEffect/element shell around mostly-shared
// leaves. r itself is never mutated.
func compressReducedState(r *pb.ReducedEffect) *pb.ReducedEffect {
	out := r
	owned := false
	ensureOwned := func() {
		if !owned {
			out = cloneReduced(r)
			owned = true
		}
	}

	if compressibleData(r.Scalar) {
		ensureOwned()
		compressDataValue(out.Scalar) // cloneReduced deep-copies Scalar
	}
	for id, elem := range r.NetAdds {
		if compressibleData(elem.Data) {
			ensureOwned()
			out.NetAdds[id] = compressedElement(elem)
		}
	}
	for i, elem := range r.OrderedElements {
		if compressibleData(elem.Data) {
			ensureOwned()
			out.OrderedElements[i] = compressedElement(elem)
		}
	}
	return out
}

// compressibleData reports whether compressDataValue could change d — the
// cheap pre-scan that lets state walks skip cloning when nothing qualifies.
// It doesn't pre-run the codec, so a value can qualify here and still stay
// on the raw arm when compression loses.
func compressibleData(d *pb.DataEffect) bool {
	if d == nil {
		return false
	}
	v, ok := d.Value.(*pb.DataEffect_Raw)
	return ok && len(v.Raw) >= compressMinBytes
}

// compressedElement is the replace-pattern twin of the reduce pipeline's
// element updates: a new element around a compressed copy of the data,
// leaving the shared original untouched.
func compressedElement(elem *pb.ReducedElement) *pb.ReducedElement {
	data := cloneData(elem.Data)
	compressDataValue(data)
	return &pb.ReducedElement{
		Data:           data,
		Hlc:            elem.Hlc,
		NodeId:         elem.NodeId,
		ForkChoiceHash: elem.ForkChoiceHash,
		ExpiresAt:      elem.ExpiresAt,
	}
}

// InflatedSizeDelta is the byte count value compression hid from this
// effect's marshal: Σ (raw_len − len(stored)) over every compressed value.
// Billing adds it to the marshal length so raw_size keeps meaning "the
// customer's bytes" no matter how well their data compresses.
func InflatedSizeDelta(eff *pb.Effect) uint64 {
	var delta uint64
	add := func(d *pb.DataEffect) {
		cv := d.GetCompressed()
		if cv != nil && cv.RawLen > uint64(len(cv.Data)) {
			delta += cv.RawLen - uint64(len(cv.Data))
		}
	}
	if data := eff.GetData(); data != nil {
		add(data)
		return delta
	}
	if snap := eff.GetSnapshot(); snap != nil && snap.State != nil {
		if snap.State.Scalar != nil {
			add(snap.State.Scalar)
		}
		for _, elem := range snap.State.NetAdds {
			add(elem.Data)
		}
		for _, elem := range snap.State.OrderedElements {
			add(elem.Data)
		}
	}
	return delta
}
