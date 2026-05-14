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
	"github.com/swytchdb/swytch/keytrie"
)

// SafetyMode determines write behavior during network partitions.
type SafetyMode int

const (
	SafeMode   SafetyMode = iota // blocks writes when quorum is unreachable
	UnsafeMode                   // allows writes during partitions, forming branches
)

// KeyRangeRule maps a key pattern to a safety mode.
type KeyRangeRule struct {
	Pattern string
	Mode    SafetyMode
}

// EngineConfig holds configuration for creating an Engine.
type EngineConfig struct {
	NodeID        pb.NodeID
	Index         keytrie.KeyIndex
	Cache         StateCache      // nil disables caching
	Broadcaster   Broadcaster     // nil for standalone
	RTTProvider   PeerRTTProvider // nil disables RTT-based leader selection
	DefaultMode   SafetyMode
	KeyRangeRules []KeyRangeRule
	MemoryLimit   int64 // memory budget for effect cache (0 = 10MB default)
	// MemoryLimitPercent, when non-zero, expresses the cache budget as a
	// fraction (0,1] of the memory available to this process. Takes
	// precedence over MemoryLimit. The cache enforces this via a live
	// re-evaluating tick so cgroup/system changes propagate.
	MemoryLimitPercent float64
}

// ModeForKey returns the safety mode for a key. First matching rule wins.
func (c *EngineConfig) ModeForKey(key string) SafetyMode {
	for _, rule := range c.KeyRangeRules {
		if keytrie.MatchGlob(key, rule.Pattern) {
			return rule.Mode
		}
	}
	return c.DefaultMode
}
