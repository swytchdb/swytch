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

package cluster

import (
	"context"
	"testing"

	dp "github.com/swytchdb/swytch/cluster/proto/dataplane"
	"github.com/swytchdb/swytch/effects"
)

// filterFrame marshals a CuckooChain over the given PRF names into the wire
// bytes a KeyFilter frame carries.
func filterFrame(t *testing.T, names ...[]byte) []byte {
	t.Helper()
	var cc effects.CuckooChain
	for _, n := range names {
		cc.Add(string(n))
	}
	b, err := cc.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal filter: %v", err)
	}
	return b
}

// TestCloudMayHoldGate covers the read-miss filter gate: open with no filter,
// closed for names absent from both chains, open for cloud-held names, and
// open for our own uploads even when the cloud's push doesn't reflect them yet.
func TestCloudMayHoldGate(t *testing.T) {
	keyNameKey := DeriveKeyNameKey("gate-test-secret")
	held := CloudKeyName(keyNameKey, []byte("held"))
	absent := CloudKeyName(keyNameKey, []byte("absent"))
	ours := CloudKeyName(keyNameKey, []byte("ours"))

	cs := &CloudSync{keyNameKey: keyNameKey}

	// No filter received yet: every name consults.
	if !cs.cloudMayHold(absent) {
		t.Fatal("gate must stay open before any filter frame arrives")
	}

	cs.handleFilter(&dp.KeyFilter{Filter: filterFrame(t, held)})
	if !cs.cloudMayHold(held) {
		t.Fatal("cloud-held name must pass the gate")
	}
	if cs.cloudMayHold(absent) {
		t.Fatal("name absent from the filter must free-miss")
	}

	// Our own upload stays consultable before the cloud's push reflects it.
	cs.filterMu.Lock()
	cs.filterOwn.Add(string(ours))
	cs.filterMu.Unlock()
	if !cs.cloudMayHold(ours) {
		t.Fatal("own-uploaded name must pass the gate")
	}

	// A replacement frame wins wholesale, but own uploads remain visible.
	cs.handleFilter(&dp.KeyFilter{Filter: filterFrame(t, absent)})
	if !cs.cloudMayHold(absent) || cs.cloudMayHold(held) {
		t.Fatal("a new frame must replace the previous filter wholesale")
	}
	if !cs.cloudMayHold(ours) {
		t.Fatal("own uploads must survive a bulk replacement")
	}
}

// TestHandleFilterUndecodableKeepsPrevious: a garbage frame is dropped and the
// prior filter's verdicts stand.
func TestHandleFilterUndecodableKeepsPrevious(t *testing.T) {
	keyNameKey := DeriveKeyNameKey("gate-test-secret")
	held := CloudKeyName(keyNameKey, []byte("held"))

	cs := &CloudSync{keyNameKey: keyNameKey}
	cs.handleFilter(&dp.KeyFilter{Filter: filterFrame(t, held)})
	cs.handleFilter(&dp.KeyFilter{Filter: []byte{0xff, 0x00, 0xba, 0xad}})

	if !cs.cloudMayHold(held) {
		t.Fatal("undecodable frame must not replace the previous filter")
	}
	if cs.cloudMayHold(CloudKeyName(keyNameKey, []byte("absent"))) {
		t.Fatal("undecodable frame must not open the gate")
	}
}

// TestCloudTipsGatedSkipsRPC: a filter-negative CloudTips returns nil without
// touching the network — the nil client proves it, since any RPC attempt
// would panic.
func TestCloudTipsGatedSkipsRPC(t *testing.T) {
	keyNameKey := DeriveKeyNameKey("gate-test-secret")
	cs := &CloudSync{keyNameKey: keyNameKey}
	cs.handleFilter(&dp.KeyFilter{Filter: filterFrame(t, CloudKeyName(keyNameKey, []byte("held")))})

	tips, err := cs.CloudTips(context.Background(), "absent")
	if err != nil {
		t.Fatalf("gated CloudTips errored: %v", err)
	}
	if tips != nil {
		t.Fatalf("gated CloudTips returned tips: %v", tips)
	}
}
