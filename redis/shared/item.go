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

// ValueType represents the type of Redis value
type ValueType byte

const (
	TypeNone   ValueType = iota // Key doesn't exist
	TypeString                  // String value
	TypeList                    // List value
	TypeHash                    // Hash value
	TypeSet                     // Set value (future)
	TypeZSet                    // Sorted set value (future)
	TypeCounter
	TypeStream // Stream value
	TypeHLL    // HyperLogLog value
)

// String returns the Redis type name
func (t ValueType) String() string {
	switch t {
	case TypeNone:
		return "none"
	case TypeString, TypeCounter, TypeHLL:
		// Redis reports counters and HyperLogLogs as strings.
		return "string"
	case TypeList:
		return "list"
	case TypeHash:
		return "hash"
	case TypeSet:
		return "set"
	case TypeZSet:
		return "zset"
	case TypeStream:
		return "stream"
	default:
		return unknownName
	}
}

// RedisValue represents a Redis value with expiration support
type RedisValue struct {
	Type              ValueType
	Data              []byte       // For strings
	List              *ListValue   // For lists
	Hash              *HashValue   // For hashes
	Set               *SetValue    // For sets
	ZSet              *ZSetValue   // For sorted sets
	Stream            *StreamValue // For streams
	HLL               *HLLValue    // For HyperLogLog
	CounterDeltaInt   int64
	CounterDeltaFloat float64
	Exptime           int64 // Unix millisecond timestamp, 0 = no expiration
	CreatedAt         int64 // Unix nanosecond timestamp when created
}

// ListValue tracks list sequence numbers for persistent storage.
type ListValue struct {
	// Sequence tracking for persistent storage (CRDT handler)
	// HeadSeq is the sequence number of the first element (decrements on LPUSH)
	// TailSeq is the sequence number of the last element (increments on RPUSH)
	// For an empty list: HeadSeq=0, TailSeq=-1
	HeadSeq int64
	TailSeq int64
}

// HashValue stores hash field/value pairs
type HashValue struct {
	Fields    map[string][]byte
	FieldsTTL map[string]int64 // field -> Unix millisecond timestamp, 0 = no expiration
}

// SetValue stores set members
type SetValue struct {
	Members map[string]struct{}
}

// HLLValue stores HyperLogLog data for probabilistic cardinality estimation
// Uses 16384 6-bit registers (2^14 registers) for ~0.81% standard error
type HLLValue struct {
	Registers [16384]uint8 // Each register stores a 6-bit value (0-63)
}

// ZSet helper methods

// ZAdd adds or updates members with scores
// Returns the number of new members added (not updated)
// HLL helper methods

// HLLHash computes a 64-bit hash for the HyperLogLog algorithm using MurmurHash3
func HLLHash(data []byte) uint64 {
	// MurmurHash3 64-bit finalizer constants
	const (
		c1 uint64 = 0x87c37b91114253d5
		c2 uint64 = 0x4cf5ad432745937f
	)

	var h1, h2 uint64 = 0, 0

	length := len(data)

	// Process 16-byte blocks
	nblocks := length / 16
	for i := range nblocks {
		k1 := uint64(data[i*16+0]) | uint64(data[i*16+1])<<8 | uint64(data[i*16+2])<<16 | uint64(data[i*16+3])<<24 |
			uint64(data[i*16+4])<<32 | uint64(data[i*16+5])<<40 | uint64(data[i*16+6])<<48 | uint64(data[i*16+7])<<56
		k2 := uint64(data[i*16+8]) | uint64(data[i*16+9])<<8 | uint64(data[i*16+10])<<16 | uint64(data[i*16+11])<<24 |
			uint64(data[i*16+12])<<32 | uint64(data[i*16+13])<<40 | uint64(data[i*16+14])<<48 | uint64(data[i*16+15])<<56

		k1 *= c1
		k1 = (k1 << 31) | (k1 >> 33)
		k1 *= c2
		h1 ^= k1
		h1 = (h1 << 27) | (h1 >> 37)
		h1 += h2
		h1 = h1*5 + 0x52dce729

		k2 *= c2
		k2 = (k2 << 33) | (k2 >> 31)
		k2 *= c1
		h2 ^= k2
		h2 = (h2 << 31) | (h2 >> 33)
		h2 += h1
		h2 = h2*5 + 0x38495ab5
	}

	// Process remaining bytes
	tail := data[nblocks*16:]

	var k1, k2 uint64

	switch len(tail) {
	case 15:
		k2 ^= uint64(tail[14]) << 48

		fallthrough
	case 14:
		k2 ^= uint64(tail[13]) << 40

		fallthrough
	case 13:
		k2 ^= uint64(tail[12]) << 32

		fallthrough
	case 12:
		k2 ^= uint64(tail[11]) << 24

		fallthrough
	case 11:
		k2 ^= uint64(tail[10]) << 16

		fallthrough
	case 10:
		k2 ^= uint64(tail[9]) << 8

		fallthrough
	case 9:
		k2 ^= uint64(tail[8])
		k2 *= c2
		k2 = (k2 << 33) | (k2 >> 31)
		k2 *= c1
		h2 ^= k2

		fallthrough
	case 8:
		k1 ^= uint64(tail[7]) << 56

		fallthrough
	case 7:
		k1 ^= uint64(tail[6]) << 48

		fallthrough
	case 6:
		k1 ^= uint64(tail[5]) << 40

		fallthrough
	case 5:
		k1 ^= uint64(tail[4]) << 32

		fallthrough
	case 4:
		k1 ^= uint64(tail[3]) << 24

		fallthrough
	case 3:
		k1 ^= uint64(tail[2]) << 16

		fallthrough
	case 2:
		k1 ^= uint64(tail[1]) << 8

		fallthrough
	case 1:
		k1 ^= uint64(tail[0])
		k1 *= c1
		k1 = (k1 << 31) | (k1 >> 33)
		k1 *= c2
		h1 ^= k1
	}

	// Finalization
	h1 ^= uint64(length)
	h2 ^= uint64(length)
	h1 += h2
	h2 += h1

	// fmix64
	h1 ^= h1 >> 33
	h1 *= 0xff51afd7ed558ccd
	h1 ^= h1 >> 33
	h1 *= 0xc4ceb9fe1a85ec53
	h1 ^= h1 >> 33

	h2 ^= h2 >> 33
	h2 *= 0xff51afd7ed558ccd
	h2 ^= h2 >> 33
	h2 *= 0xc4ceb9fe1a85ec53
	h2 ^= h2 >> 33

	return h1 + h2
}

// HLLRho counts the number of leading zeros + 1 in the given bits
// This is used to determine the register value for HyperLogLog
func HLLRho(bits uint64) uint8 {
	if bits == 0 {
		return 64 // All zeros
	}

	count := uint8(1)
	for (bits & 0x8000000000000000) == 0 {
		count++
		bits <<= 1
	}

	return count
}

// Add adds an element to the HyperLogLog
// Returns true if the internal registers were modified (cardinality might have increased)
func (h *HLLValue) Add(element []byte) bool {
	hash := HLLHash(element)

	// Use the first 14 bits to determine the register index
	index := hash & 0x3FFF // 16383 = 2^14 - 1

	// Use the remaining 50 bits to count leading zeros
	// We need to shift left by 14 to get the remaining bits at the top
	remainingBits := hash << 14

	// Count leading zeros + 1
	rho := min(
		// Cap at 63 since we store in a uint8 and Redis uses 6 bits
		HLLRho(remainingBits), 63)

	// Update register if new value is larger
	if rho > h.Registers[index] {
		h.Registers[index] = rho

		return true
	}

	return false
}

// Count estimates the cardinality of the set
func (h *HLLValue) Count() int64 {
	const m = 16384.0 // Number of registers

	// Compute the raw estimate using harmonic mean
	var sum float64

	zeros := 0

	for _, val := range h.Registers {
		sum += 1.0 / float64(uint64(1)<<val)
		if val == 0 {
			zeros++
		}
	}

	// alpha_m constant for m = 16384
	const alpha = 0.7213 / (1 + 1.079/m)

	estimate := alpha * m * m / sum

	// Apply corrections for small and large cardinalities
	if estimate <= 5*m/2 {
		// Small range correction using linear counting if there are zero registers
		if zeros > 0 {
			// Linear counting
			estimate = m * float64(logTable[zeros])
		}
	} else if estimate > (1<<32)/30.0 {
		// Large range correction (for very large cardinalities)
		estimate = -float64(1<<32) * logTable[int(float64(1<<32)*(1-estimate/float64(1<<32)))]
	}

	// Round to nearest integer
	if estimate < 0 {
		return 0
	}

	return int64(estimate + 0.5)
}

// logTable is a precomputed table of -log(x/16384) values for linear counting
// This avoids expensive math.Log calls
var logTable = func() [16385]float64 {
	var table [16385]float64

	for i := 0; i <= 16384; i++ {
		if i == 0 {
			table[i] = 0
		} else {
			// -log(i/16384) = log(16384/i)
			// We use natural log
			table[i] = -nativeLog(float64(i) / 16384.0)
		}
	}

	return table
}()

// nativeLog computes natural logarithm using Taylor series
func nativeLog(x float64) float64 {
	if x <= 0 {
		return -1e308 // Approximate -infinity
	}

	if x == 1 {
		return 0
	}

	// Reduce x to range [0.5, 1.5) using x = m * 2^e
	var exp int

	for x >= 2 {
		x /= 2
		exp++
	}

	for x < 0.5 {
		x *= 2
		exp--
	}

	// Now x is in [0.5, 2), use Taylor series around 1
	// log(x) = log((1+y)/(1-y)) * 2 where y = (x-1)/(x+1)
	y := (x - 1) / (x + 1)
	y2 := y * y

	// Taylor series: 2 * (y + y^3/3 + y^5/5 + y^7/7 + ...)
	result := y

	term := y
	for i := 3; i < 50; i += 2 {
		term *= y2
		result += term / float64(i)
	}

	result *= 2

	// Add back the exponent: log(m * 2^e) = log(m) + e * log(2)
	const ln2 = 0.6931471805599453

	return result + float64(exp)*ln2
}

// Merge merges other HyperLogLogs into this one
// Takes the maximum value for each register position
func (h *HLLValue) Merge(others ...*HLLValue) {
	for _, other := range others {
		if other == nil {
			continue
		}

		for i := range h.Registers {
			if other.Registers[i] > h.Registers[i] {
				h.Registers[i] = other.Registers[i]
			}
		}
	}
}
