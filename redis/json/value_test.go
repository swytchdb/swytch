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

package json

import "testing"

func TestParseSerialize_RoundTrip(t *testing.T) {
	cases := []string{
		`null`,
		`true`,
		`false`,
		`42`,
		`-17`,
		`3.5`,
		`"hello"`,
		`"a\"b\\c\n"`,
		`[]`,
		`{}`,
		`[1,2,3]`,
		`{"a":1,"b":"x","c":true,"d":null}`,
		`{"nested":{"arr":[1,{"k":"v"}],"n":2}}`,
	}
	for _, in := range cases {
		v, err := Parse([]byte(in))
		if err != nil {
			t.Fatalf("Parse(%s) error: %v", in, err)
		}
		got := string(Serialize(v))
		if got != in {
			t.Errorf("round-trip: Parse+Serialize(%s) = %s", in, got)
		}
	}
}

func TestParse_PreservesObjectOrder(t *testing.T) {
	v, err := Parse([]byte(`{"z":1,"a":2,"m":3}`))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	want := []string{"z", "a", "m"}
	if len(v.Obj) != 3 {
		t.Fatalf("got %d members", len(v.Obj))
	}
	for i, m := range v.Obj {
		if m.Key != want[i] {
			t.Fatalf("member %d = %q, want %q (order not preserved)", i, m.Key, want[i])
		}
	}
}

func TestParse_IntVsFloat(t *testing.T) {
	cases := []struct {
		in   string
		kind Kind
		name string
	}{
		{`42`, KindInt, "integer"},
		{`1.0`, KindFloat, "number"},
		{`1.5`, KindFloat, "number"},
		{`9007199254740993`, KindInt, "integer"}, // > 2^53: must stay an exact integer
	}
	for _, c := range cases {
		v, err := Parse([]byte(c.in))
		if err != nil {
			t.Fatalf("parse(%s) error: %v", c.in, err)
		}
		if v.Kind != c.kind {
			t.Errorf("Parse(%s) kind = %v, want %v", c.in, v.Kind, c.kind)
		}
		if v.TypeName() != c.name {
			t.Errorf("Parse(%s) TypeName = %q, want %q", c.in, v.TypeName(), c.name)
		}
	}
	v, _ := Parse([]byte(`9007199254740993`))
	if v.Int != 9007199254740993 {
		t.Errorf("large int lost precision: %d", v.Int)
	}
}

func TestParse_RejectsTrailingGarbage(t *testing.T) {
	if _, err := Parse([]byte(`1 2`)); err == nil {
		t.Fatalf("expected error on trailing data")
	}
	if _, err := Parse([]byte(`{`)); err == nil {
		t.Fatalf("expected error on truncated object")
	}
}

func TestSerializePretty(t *testing.T) {
	v, _ := Parse([]byte(`{"a":1,"b":[2,3]}`))
	got := string(SerializePretty(v, PrintOpts{Indent: "  ", Newline: "\n", Space: " "}))
	want := "{\n  \"a\": 1,\n  \"b\": [\n    2,\n    3\n  ]\n}"
	if got != want {
		t.Fatalf("pretty:\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
	// Empty opts == compact.
	if c := string(SerializePretty(v, PrintOpts{})); c != `{"a":1,"b":[2,3]}` {
		t.Fatalf("empty opts not compact: %s", c)
	}
}
