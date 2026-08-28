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

package shared

import (
	pb "github.com/swytchdb/engine/cluster/proto"
)

// BytesReconstructor converts a non-SCALAR snapshot into raw bytes.
// Registered per ValueType by modules (bitmap, hll) at init time.
type BytesReconstructor func(snap *pb.ReducedEffect) []byte

var bytesReconstructors = map[pb.ValueType]BytesReconstructor{}

func RegisterBytesReconstructor(vt pb.ValueType, fn BytesReconstructor) {
	bytesReconstructors[vt] = fn
}

// ReconstructBytes attempts to convert a snapshot to raw bytes.
// Returns (bytes, true) if a reconstructor exists, (nil, false) otherwise.
func ReconstructBytes(snap *pb.ReducedEffect) ([]byte, bool) {
	if snap == nil {
		return nil, false
	}
	fn, ok := bytesReconstructors[snap.TypeTag]
	if !ok {
		return nil, false
	}
	return fn(snap), true
}
