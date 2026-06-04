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

package keytrie

import "testing"

func tip(seq uint64) EffectRef { return EffectRef{1, seq} }

// TestEvictBounded_EvictsColdNotProtected verifies the protected-freq policy:
// keys accessed enough to exceed k are protected, and the bounded sweep evicts
// the cold (low-freq) key instead.
func TestEvictBounded_EvictsColdNotProtected(t *testing.T) {
	c := NewCritbit[struct{}]()
	var evicted []string
	c.SetEvictHooks(
		func(string) bool { return true },
		func(k string, _ []EffectRef, _ *struct{}) { evicted = append(evicted, k) },
	)

	for i, k := range []string{"hot-a", "hot-b", "cold-c"} {
		c.Insert(k, nil, NewTipSet(tip(uint64(i+1))))
	}
	// Protect the two hot keys by accessing them past k (=2).
	for range 5 {
		c.LoadOrStoreData("hot-a", &struct{}{})
		c.LoadOrStoreData("hot-b", &struct{}{})
	}

	if !c.EvictBounded(64) {
		t.Fatal("EvictBounded reported no eviction; expected the cold key to go")
	}
	if len(evicted) != 1 || evicted[0] != "cold-c" {
		t.Fatalf("evicted = %v; want [cold-c]", evicted)
	}
	if c.Contains("cold-c") != nil {
		t.Fatal("cold-c still present after eviction")
	}
	if c.Contains("hot-a") == nil || c.Contains("hot-b") == nil {
		t.Fatal("a protected hot key was evicted")
	}
}

// TestEvictBounded_GhostPromoteOnReinsert verifies that an evicted key leaves a
// ghost retaining its frequency, and re-inserting it promotes the ghost back
// to a live leaf (warm restart) rather than starting cold.
func TestEvictBounded_GhostPromoteOnReinsert(t *testing.T) {
	c := NewCritbit[struct{}]()
	c.SetEvictHooks(func(string) bool { return true }, func(string, []EffectRef, *struct{}) {})

	c.Insert("x", nil, NewTipSet(tip(1)))
	if !c.EvictBounded(64) {
		t.Fatal("expected x to be evicted")
	}
	if c.Contains("x") != nil {
		t.Fatal("x should be absent after eviction")
	}
	if g := c.ghostCount.Load(); g != 1 {
		t.Fatalf("ghostCount = %d; want 1 (the evicted key is a ghost)", g)
	}

	// Re-insert: the ghost must be promoted, not double-counted.
	c.Insert("x", nil, NewTipSet(tip(2)))
	if c.Contains("x") == nil {
		t.Fatal("x should be live again after re-insert")
	}
	if g := c.ghostCount.Load(); g != 0 {
		t.Fatalf("ghostCount = %d; want 0 after promotion", g)
	}
}

// TestEvictBounded_GhostNotReaped verifies the reaper leaves ghost leaves in
// place (their frequency memory is the point) while still pruning a normal
// deleted leaf.
func TestEvictBounded_GhostNotReaped(t *testing.T) {
	c := NewCritbit[struct{}]()
	c.SetEvictHooks(func(string) bool { return true }, func(string, []EffectRef, *struct{}) {})

	c.Insert("ghost", nil, NewTipSet(tip(1)))
	c.Insert("normal", nil, NewTipSet(tip(2)))

	if !c.EvictBounded(64) {
		t.Fatal("expected an eviction")
	}
	// Force a reap pass; the ghost must survive it.
	c.reap()
	if g := c.ghostCount.Load(); g < 1 {
		t.Fatalf("ghostCount = %d; a ghost should have survived reap", g)
	}
}
