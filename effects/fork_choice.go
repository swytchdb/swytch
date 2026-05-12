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
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"github.com/swytchdb/swytch/cluster/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ComputeForkChoiceHash computes SHA-256(nodeID_LE8 || hlc_LE8).
// The result is a deterministic 32-byte hash used for fork-choice tiebreaking.
// Lower hash wins, eliminating systematic advantage from raw HLC comparison.
func ComputeForkChoiceHash(nodeID proto.NodeID, hlc *timestamppb.Timestamp) []byte {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[:8], uint64(nodeID))
	binary.LittleEndian.PutUint64(buf[8:], uint64(hlc.AsTime().UnixNano()))
	h := sha256.Sum256(buf[:])
	return h[:]
}

// ForkChoiceLess returns true if hash a is lexicographically less than hash b.
// Lower hash wins the fork-choice election.
func ForkChoiceLess(a, b []byte) bool {
	return bytes.Compare(a, b) < 0
}
