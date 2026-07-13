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
	"testing"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
)

func TestSetOnLocalEffectWaitsForInflightCallback(t *testing.T) {
	engine := NewEngine(EngineConfig{NodeID: 1})
	defer func() {
		if err := engine.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
	}()

	entered := make(chan struct{})
	release := make(chan struct{})
	engine.SetOnLocalEffect(func(Tip, *pb.Effect) {
		close(entered)
		<-release
	})

	fired := make(chan struct{})
	go func() {
		engine.fireLocalEffect(Tip{1, 1}, &pb.Effect{})
		close(fired)
	}()
	<-entered

	detached := make(chan struct{})
	go func() {
		engine.SetOnLocalEffect(nil)
		close(detached)
	}()
	select {
	case <-detached:
		t.Fatal("hook detached while its callback was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	<-fired
	<-detached

	// Once detachment returns, later mints must not enter the old callback.
	engine.fireLocalEffect(Tip{1, 2}, &pb.Effect{})
}
