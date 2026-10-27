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

package redis

import (
	"context"
	"testing"

	"github.com/swytchdb/engine/effects"
)

func TestEffectiveMemoryLimitAbsolute(t *testing.T) {
	const absolute = int64(256 << 20)

	if got := effectiveMemoryLimit(absolute, nil); got != absolute {
		t.Fatalf("effectiveMemoryLimit(%d, nil) = %d, want %d", absolute, got, absolute)
	}
}

func TestEffectiveMemoryLimitPercentResolvedByEngine(t *testing.T) {
	engine, err := effects.OpenEngine(context.Background(), effects.EngineConfig{
		MemoryLimitPercent: 0.5,
	})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })

	target := engine.MemoryTarget()
	if target <= 0 {
		t.Fatalf("engine resolved percentage to %d bytes, want > 0", target)
	}

	if got := effectiveMemoryLimit(0, engine); got != target {
		t.Fatalf("effectiveMemoryLimit(0, engine) = %d, want engine target %d", got, target)
	}
}

func TestEffectiveMemoryLimitAbsoluteUnchangedByEngine(t *testing.T) {
	const absolute = int64(128 << 20)

	engine, err := effects.OpenEngine(context.Background(), effects.EngineConfig{
		MemoryLimit: absolute,
	})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })

	if got := engine.MemoryTarget(); got != absolute {
		t.Fatalf("engine target = %d, want %d", got, absolute)
	}

	if got := effectiveMemoryLimit(absolute, engine); got != absolute {
		t.Fatalf("effectiveMemoryLimit(%d, engine) = %d, want %d", absolute, got, absolute)
	}
}
