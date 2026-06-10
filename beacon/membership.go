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

package beacon

import (
	"encoding/binary"

	pb "github.com/swytchdb/swytch/cluster/proto"
)

// MembershipKey is the reserved effects key for cluster membership.
const MembershipKey = "__swytch:members"

// Member represents a single cluster node.
type Member struct {
	NodeID uint64
	Addr   string // host:port
}

// nodeIDBytes encodes a NodeID as 8-byte little-endian for use as a KEYED element ID.
func nodeIDBytes(nodeID uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, nodeID)
	return b
}

// nodeIDFromBytes decodes a NodeID from 8-byte little-endian element ID.
func nodeIDFromBytes(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}

// buildMemberInsert creates an INSERT_OP DataEffect for initial membership registration.
// This is the equivalent of HSET __swytch:members <nodeID> <addr>.
func buildMemberInsert(nodeID uint64, addr string) *pb.Effect {
	return &pb.Effect{
		Key: []byte(MembershipKey),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Placement:  pb.Placement_PLACE_NONE,
			Id:         nodeIDBytes(nodeID),
			Value:      &pb.DataEffect_Raw{Raw: []byte(addr)},
		}},
	}
}

// buildMemberTypeTag creates a MetaEffect that tags the key as a hash type.
func buildMemberTypeTag() *pb.Effect {
	return &pb.Effect{
		Key:  []byte(MembershipKey),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_HASH}},
	}
}

// buildMemberRemove creates a REMOVE_OP DataEffect for graceful departure.
func buildMemberRemove(nodeID uint64) *pb.Effect {
	return &pb.Effect{
		Key: []byte(MembershipKey),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_KEYED,
			Id:         nodeIDBytes(nodeID),
		}},
	}
}


// parseMembership extracts the member list from a reduced snapshot of __swytch:members.
func parseMembership(reduced *pb.ReducedEffect) []Member {
	if reduced == nil || reduced.NetAdds == nil {
		return nil
	}

	members := make([]Member, 0, len(reduced.NetAdds))
	for idStr, elem := range reduced.NetAdds {
		nodeID := nodeIDFromBytes([]byte(idStr))
		if nodeID == 0 {
			continue
		}
		addr := string(elem.Data.GetRaw())
		if addr == "" {
			continue
		}
		members = append(members, Member{
			NodeID: nodeID,
			Addr:   addr,
		})
	}
	return members
}
