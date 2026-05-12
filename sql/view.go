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
	"fmt"
	"sort"

	"zombiezen.com/go/sqlite"

	"github.com/swytchdb/swytch/effects"
)

// viewSchema is the on-disk representation of a CREATE VIEW.
//
// Body is the raw text that appears after the view name in the
// original CREATE VIEW statement — typically "AS <select>", or
// "(col1, col2) AS <select>" when the optional projection list is
// used. We preserve it verbatim so SQLite sees byte-identical SQL on
// every session that replays the view, which matters for things like
// string-literal quoting and whitespace inside the SELECT body.
type viewSchema struct {
	Name string
	Body string
}

// emitView writes the view's body under the v/<name> key and marks
// the name present in the replicated view list V. Caller is
// responsible for transaction framing.
func emitView(ctx *effects.Context, v *viewSchema) error {
	if v.Name == "" {
		return fmt.Errorf("sql: emitView: empty name")
	}
	if v.Body == "" {
		return fmt.Errorf("sql: emitView: view %q has empty body", v.Name)
	}
	if err := emitRawField(ctx, viewMetaKey(v.Name), viewFieldSQL, []byte(v.Body)); err != nil {
		return err
	}
	return emitRawField(ctx, viewListKey, v.Name, []byte{1})
}

// emitViewRemove REMOVEs the metadata fields plus the registry entry
// for a view. Caller is responsible for transaction framing.
func emitViewRemove(ctx *effects.Context, name string) error {
	if err := emitKeyedRemove(ctx, viewMetaKey(name), viewFieldSQL); err != nil {
		return err
	}
	return emitKeyedRemove(ctx, viewListKey, name)
}

// loadView reads a view's metadata, returning ErrNoSuchView if the
// view isn't registered.
func loadView(ctx *effects.Context, name string) (*viewSchema, error) {
	reduced, _, err := ctx.GetSnapshot(viewMetaKey(name))
	if err != nil {
		return nil, fmt.Errorf("sql: read view %q: %w", name, err)
	}
	if reduced == nil || len(reduced.NetAdds) == 0 {
		return nil, ErrNoSuchView
	}
	el, ok := reduced.NetAdds[viewFieldSQL]
	if !ok || el.Data == nil {
		return nil, fmt.Errorf("sql: view %q missing sql field", name)
	}
	return &viewSchema{Name: name, Body: string(el.Data.GetRaw())}, nil
}

// listViews returns every view name currently present in the
// replicated V key, sorted for determinism.
func listViews(ctx *effects.Context) ([]string, error) {
	reduced, _, err := ctx.GetSnapshot(viewListKey)
	if err != nil {
		return nil, fmt.Errorf("sql: read view list: %w", err)
	}
	if reduced == nil || len(reduced.NetAdds) == 0 {
		return nil, nil
	}
	names := make([]string, 0, len(reduced.NetAdds))
	for name := range reduced.NetAdds {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// registerView issues CREATE VIEW <quoted-name> <body> on the given
// SQLite conn. The body carries its own "AS <select>" (and optional
// projection list) so we only need to quote the name ourselves — any
// quoting already in the body is preserved.
func registerView(conn *sqlite.Conn, v *viewSchema) error {
	return execSimple(conn, "CREATE VIEW "+quoteIdent(v.Name)+" "+v.Body)
}
