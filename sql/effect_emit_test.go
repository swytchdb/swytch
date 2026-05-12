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

package sql

import (
	"testing"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/keytrie"
)

// TestEngineAcceptsObservationEffect proves that the effects engine
// tolerates an ObservationEffect end-to-end: Emit + Flush don't
// error, and a subsequent ScanKeys finds the target key. The
// engine's reduce logic silently ignores unknown kinds, which is
// the correct behaviour until Phase 5 fork-choice logic lands.
func TestEngineAcceptsObservationEffect(t *testing.T) {
	eng := effects.NewEngine(effects.EngineConfig{
		NodeID:      1,
		Index:       keytrie.New(),
		DefaultMode: effects.SafeMode,
	})
	defer func() {
		if err := eng.Close(); err != nil {
			t.Errorf("close engine: %v", err)
		}
	}()

	ctx := eng.NewContext()
	pred := PAnd(
		PCmp(0, OpEq, IntVal(5)),
		PNot(PIsNull(1)),
	)
	obs := &pb.Effect{
		Key: []byte("s/users"),
		Kind: &pb.Effect_Observation{Observation: &pb.ObservationEffect{
			Predicate: predicateToProto(pred),
		}},
	}
	if err := ctx.Emit(obs); err != nil {
		t.Fatalf("emit observation: %v", err)
	}
	if err := ctx.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// After flush the key should be visible to ScanKeys.
	var found bool
	eng.ScanKeys("", "s/users", func(key string) bool {
		if key == "s/users" {
			found = true
		}
		return true
	})
	if !found {
		t.Error("s/users should be in the key index after observation emit")
	}
}

// TestEngineAcceptsRowWriteEffect is the same smoke test for
// RowWriteEffect.
func TestEngineAcceptsRowWriteEffect(t *testing.T) {
	eng := effects.NewEngine(effects.EngineConfig{
		NodeID:      1,
		Index:       keytrie.New(),
		DefaultMode: effects.SafeMode,
	})
	defer func() {
		if err := eng.Close(); err != nil {
			t.Errorf("close engine: %v", err)
		}
	}()

	ctx := eng.NewContext()
	rw := RowWrite{
		Table: "users",
		PK:    []byte{0, 0, 0, 0, 0, 0, 0, 7},
		Kind:  WriteInsert,
		Cols:  []TypedValue{IntVal(7), TextVal("alice")},
	}
	eff := &pb.Effect{
		Key:  []byte("s/users"),
		Kind: &pb.Effect_RowWrite{RowWrite: rowWriteToProto(rw)},
	}
	if err := ctx.Emit(eff); err != nil {
		t.Fatalf("emit row-write: %v", err)
	}
	if err := ctx.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// TestWritePathEmitsRowWriteEffect is an indirect check: after a
// full CRUD cycle through the sql package, the table-identity key's
// snapshot still decodes cleanly (none of the RowWriteEffects we
// emit at s/<table> pollute the schema reduction, since reduce
// ignores unknown kinds). If an overlapping schema read returned a
// corrupt snapshot, loadSchema would fail.
func TestWritePathEmitsRowWriteEffect(t *testing.T) {
	_, db := startTestServer(t)
	mustExec(t, db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

	// Do each write-path operation in turn.
	mustExec(t, db, "INSERT INTO users VALUES (1, 'alice')")
	mustExec(t, db, "INSERT INTO users VALUES (2, 'bob')")
	mustExec(t, db, "UPDATE users SET name = 'ALICE' WHERE id = 1")
	mustExec(t, db, "UPDATE users SET id = 99 WHERE id = 2")
	mustExec(t, db, "DELETE FROM users WHERE id = 1")

	// After all that, a fresh query through a new session must
	// still see a coherent schema and the surviving row. If
	// RowWriteEffects at s/users had been reduced into the schema
	// metadata, this would break.
	var name string
	if err := db.QueryRow("SELECT name FROM users WHERE id = 99").Scan(&name); err != nil {
		t.Fatalf("select: %v", err)
	}
	if name != "bob" {
		t.Errorf("got name=%q, want bob", name)
	}
}
