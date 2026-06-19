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
	"strconv"
	"testing"

	pb "github.com/swytchdb/swytch/cluster/proto"
)

// pKeyedSized builds a KEYED partition with m elements, so cloning it costs
// O(m) (a representative non-trivial nested container).
func pKeyedSized(m int, fch byte) *pb.ReducedEffect {
	na := make(map[string]*pb.ReducedElement, m)
	for i := range m {
		na["e"+strconv.Itoa(i)] = &pb.ReducedElement{
			Data: &pb.DataEffect{
				Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_KEYED,
				Id: []byte("e" + strconv.Itoa(i)), Value: &pb.DataEffect_Raw{Raw: []byte("v")},
			},
		}
	}
	return &pb.ReducedEffect{Collection: pb.CollectionKind_KEYED, NetAdds: na, ForkChoiceHash: []byte{fch}}
}

// rootKeyedInsert is a root (virtual="") write — it touches only the root
// partition, leaving every nested partition untouched (the carry-over case).
func rootKeyedInsert(id string, fch byte) *pb.Effect {
	return &pb.Effect{
		ForkChoiceHash: []byte{fch},
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED, Id: []byte(id),
			Value: &pb.DataEffect_Raw{Raw: []byte("x")},
		}},
	}
}

// seedWithPartitions builds a root object carrying nParts partitions of m
// elements each.
func seedWithPartitions(nParts, m int) *pb.ReducedEffect {
	parts := make(map[string]*pb.ReducedEffect, nParts)
	for i := range nParts {
		parts["p"+strconv.Itoa(i)] = pKeyedSized(m, byte(i))
	}
	return branchWithPartitions(0x01, parts)
}

// accumulate replays the reconstruct per-effect pattern (snapshot.go:1008):
// result = ReduceChain(result, []{eff}) for each effect in the chain.
func accumulate(seed *pb.ReducedEffect, chain []*pb.Effect) *pb.ReducedEffect {
	acc := seed
	for _, eff := range chain {
		acc = ReduceChain(acc, []*pb.Effect{eff})
	}
	return acc
}

// BenchmarkReducePartitionedChain measures reconstructing a partitioned key over
// a chain of root writes. With the carry-by-reference fix, per-effect cost is
// independent of partition SIZE (untouched partitions are shared, not cloned);
// run with -benchmem and vary m to confirm allocs/op does not scale with m.
func BenchmarkReducePartitionedChain(b *testing.B) {
	const nParts, chainLen = 64, 200
	for _, m := range []int{8, 256} {
		seed := seedWithPartitions(nParts, m)
		chain := make([]*pb.Effect, chainLen)
		for i := range chain {
			chain[i] = rootKeyedInsert("k"+strconv.Itoa(i), byte(i))
		}
		b.Run("elemsPerPartition="+strconv.Itoa(m), func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				_ = accumulate(seed, chain)
			}
		})
	}
}
