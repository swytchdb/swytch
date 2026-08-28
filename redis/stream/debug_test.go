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

package stream

import (
	"fmt"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/swytch/redis/shared"
)

func TestDebugGroupCreate(t *testing.T) {
	env := newTestEnv()

	// Step 1: XADD
	result := xadd(t, env, "mystream", "1-0", "field", "value")
	t.Logf("XADD result: %q", result)

	// Verify stream exists
	snap, tips, err := env.ctx.GetSnapshot("mystream")
	t.Logf("After XADD: snap=%v tips=%v err=%v", snap != nil, tips, err)
	if snap != nil {
		t.Logf("  Collection=%v TypeTag=%v OrderedElements=%d NetAdds=%d",
			snap.Collection, snap.TypeTag, len(snap.OrderedElements), len(snap.NetAdds))
	}

	// Step 2: First XGROUP CREATE
	cmd := makeCmd(env, "CREATE", "mystream", "mygroup", "0")
	valid, _, runner := handleXGroup(cmd, env.w, nil)
	t.Logf("XGROUP CREATE valid=%v", valid)
	if runner != nil {
		runner()
		flushErr := env.ctx.Flush()
		t.Logf("XGROUP CREATE flush err=%v", flushErr)
	}
	result1 := env.buf.String()
	t.Logf("XGROUP CREATE result: %q", result1)
	env.buf.Reset()

	// Verify stream still exists
	snap2, tips2, err2 := env.ctx.GetSnapshot("mystream")
	t.Logf("After XGROUP CREATE: snap=%v tips=%v err=%v", snap2 != nil, tips2, err2)
	if snap2 != nil {
		t.Logf("  Collection=%v TypeTag=%v OrderedElements=%d NetAdds=%d Op=%v",
			snap2.Collection, snap2.TypeTag, len(snap2.OrderedElements), len(snap2.NetAdds), snap2.Op)
	}

	// Check what getStreamSnapshot returns
	cmd2 := makeCmd(env, "dummy")
	snapX, _, exists, wrongType, errX := getStreamSnapshot(cmd2, "mystream")
	t.Logf("getStreamSnapshot: snap=%v exists=%v wrongType=%v err=%v", snapX != nil, exists, wrongType, errX)
	if snapX != nil {
		t.Logf("  Collection=%v TypeTag=%v OrderedElements=%d NetAdds=%d Op=%v",
			snapX.Collection, snapX.TypeTag, len(snapX.OrderedElements), len(snapX.NetAdds), snapX.Op)
	}
}

func makeCmd(env *testEnv, args ...string) *shared.Command {
	byteArgs := make([][]byte, len(args))
	for i, a := range args {
		byteArgs[i] = []byte(a)
	}
	return &shared.Command{Args: byteArgs, Runtime: env.eng, Context: env.ctx}
}

func init() {
	_ = fmt.Sprint
	_ = pb.CollectionKind_ORDERED
}
