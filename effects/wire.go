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
	"encoding/hex"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"google.golang.org/protobuf/proto"
)

// MarshalEffect serializes an Effect to its wire bytes, hex-encoding any
// map<string, X> keys inside an embedded ReducedEffect (Snapshot state).
// Proto3 requires string map keys to be valid UTF-8, but Redis-style
// collections (hashes, sets, zsets, streams) accept arbitrary byte
// sequences as element IDs — and the cluster membership encodes a uint64
// as raw little-endian bytes. Without encoding, proto.Marshal silently
// rejects snapshots whose state has any binary-keyed elements.
//
// The encoding is hex (always-on, no discriminator). The reverse step is
// in UnmarshalEffect. The original *pb.Effect is not mutated; if any
// sanitization is required the snapshot state is cloned first.
func MarshalEffect(eff *pb.Effect) ([]byte, error) {
	if needsKeySanitize(eff) {
		clone := proto.Clone(eff).(*pb.Effect)
		sanitizeReducedKeys(clone.GetSnapshot().State)
		return proto.Marshal(clone)
	}
	return proto.Marshal(eff)
}

// UnmarshalEffect parses wire bytes into eff and reverses the hex encoding
// applied by MarshalEffect on any embedded ReducedEffect (Snapshot state).
func UnmarshalEffect(data []byte, eff *pb.Effect) error {
	if err := proto.Unmarshal(data, eff); err != nil {
		return err
	}
	if snap := eff.GetSnapshot(); snap != nil && snap.State != nil {
		desanitizeReducedKeys(snap.State)
	}
	return nil
}

// needsKeySanitize reports whether marshalling eff requires hex-encoding
// any map keys (i.e. whether the effect carries a non-empty snapshot state).
func needsKeySanitize(eff *pb.Effect) bool {
	snap := eff.GetSnapshot()
	if snap == nil || snap.State == nil {
		return false
	}
	return len(snap.State.NetAdds) > 0 || len(snap.State.NetRemoves) > 0
}

// sanitizeReducedKeys hex-encodes the keys of NetAdds and NetRemoves in
// place. Mutates r — callers must clone first if r is shared.
func sanitizeReducedKeys(r *pb.ReducedEffect) {
	if len(r.NetAdds) > 0 {
		out := make(map[string]*pb.ReducedElement, len(r.NetAdds))
		for k, v := range r.NetAdds {
			out[hex.EncodeToString([]byte(k))] = v
		}
		r.NetAdds = out
	}
	if len(r.NetRemoves) > 0 {
		out := make(map[string]bool, len(r.NetRemoves))
		for k, v := range r.NetRemoves {
			out[hex.EncodeToString([]byte(k))] = v
		}
		r.NetRemoves = out
	}
}

// desanitizeReducedKeys hex-decodes the keys of NetAdds and NetRemoves in
// place. Keys that do not decode cleanly are kept verbatim (they predate
// the wire-encoding scheme or are corrupt — either way, dropping them
// silently would lose data).
func desanitizeReducedKeys(r *pb.ReducedEffect) {
	if len(r.NetAdds) > 0 {
		out := make(map[string]*pb.ReducedElement, len(r.NetAdds))
		for k, v := range r.NetAdds {
			if b, err := hex.DecodeString(k); err == nil {
				out[string(b)] = v
			} else {
				out[k] = v
			}
		}
		r.NetAdds = out
	}
	if len(r.NetRemoves) > 0 {
		out := make(map[string]bool, len(r.NetRemoves))
		for k, v := range r.NetRemoves {
			if b, err := hex.DecodeString(k); err == nil {
				out[string(b)] = v
			} else {
				out[k] = v
			}
		}
		r.NetRemoves = out
	}
}
