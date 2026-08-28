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
	"encoding/binary"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/crdt"
)

var (
	EffectsNodeID pb.NodeID
	HlcClock      *crdt.HLC
)

// InitEffectsGlobals initializes the global HLC clock and node ID.
// Must be called at server startup before any commands are processed.
func InitEffectsGlobals(nodeID pb.NodeID) {
	EffectsNodeID = nodeID
	HlcClock = crdt.NewHLC()
}

// GetNodeID returns this node's ID.
func GetNodeID() pb.NodeID {
	return EffectsNodeID
}

// NextHLC returns a monotonically increasing timestamp.
func NextHLC() time.Time {
	if HlcClock == nil {
		return time.Now()
	}
	return HlcClock.Now()
}

// EncodeElementID encodes an (hlc, nodeID) pair into a 16-byte element ID.
func EncodeElementID(hlc time.Time, nodeID pb.NodeID) []byte {
	id := make([]byte, 16)
	binary.LittleEndian.PutUint64(id, uint64(hlc.UnixNano()))
	binary.LittleEndian.PutUint64(id[8:], uint64(nodeID))
	return id
}
