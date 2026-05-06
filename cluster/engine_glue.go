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
	"encoding/binary"
	"fmt"
	"log/slog"

	clox "github.com/swytchdb/cache/cache"
	pb "github.com/swytchdb/cache/cluster/proto"
	"github.com/swytchdb/cache/effects"
)

// EngineEffectHandler adapts an *effects.Engine to the EffectHandler
// interface used by PeerManager. It forwards OffsetNotify / Nack
// frames straight to the engine; there's no front-end-specific logic.
// Previously duplicated in the redis package; shared here so every
// transport (redis, sql, future ones) builds a PeerManager the same
// way.
type EngineEffectHandler struct {
	engine *effects.Engine
}

// NewEngineEffectHandler wraps the given engine so it satisfies the
// EffectHandler interface.
func NewEngineEffectHandler(engine *effects.Engine) *EngineEffectHandler {
	return &EngineEffectHandler{engine: engine}
}

// HandleOffsetNotify delegates to the engine's HandleRemote. The
// engine handles log storage, keytrie update, cache invalidation, NACK
// generation, and notification callbacks.
func (h *EngineEffectHandler) HandleOffsetNotify(notify *pb.OffsetNotify) ([]*pb.NackNotify, error) {
	if h.engine == nil {
		return nil, nil
	}
	nacks, err := h.engine.HandleRemote(notify)
	if err != nil {
		slog.Warn("engine HandleRemote failed",
			"key", notify.Key,
			"offset", notify.Origin.GetOffset(),
			"error", err)
		return nil, err
	}
	return nacks, nil
}

// HandleNack forwards NACKs to the engine.
func (h *EngineEffectHandler) HandleNack(nack *pb.NackNotify) error {
	if h.engine != nil {
		return h.engine.HandleNack(nack)
	}
	return nil
}

// EngineLogReader adapts the engine's effect cache to the LogReader
// interface. Previously lived in the redis package; shared here.
//
// Wire format produced by ReadEffect:
//
//	[4 bytes LE keyLen][keyLen bytes of key][proto-marshalled effect]
//
// The receiver reconstructs the full log entry including the key,
// which is stored separately from the effect data in the log.
type EngineLogReader struct {
	effectCache *clox.CloxCache[effects.Tip, *pb.Effect]
}

// NewEngineLogReader wraps an effect cache.
func NewEngineLogReader(cache *clox.CloxCache[effects.Tip, *pb.Effect]) *EngineLogReader {
	return &EngineLogReader{effectCache: cache}
}

// ReadEffect looks the effect up in the cache and returns it in wire
// format.
func (r *EngineLogReader) ReadEffect(ref *pb.EffectRef) ([]byte, error) {
	if r.effectCache == nil {
		return nil, fmt.Errorf("effect cache not initialized")
	}
	key := effects.Tip{ref.NodeId, ref.Offset}
	eff, ok := r.effectCache.Get(key, 0)
	if !ok {
		return nil, fmt.Errorf("effect not found at %v", ref)
	}
	return marshalEffectToWire(eff)
}

// marshalEffectToWire serializes an Effect to the wire format.
func marshalEffectToWire(eff *pb.Effect) ([]byte, error) {
	protoData, err := effects.MarshalEffect(eff)
	if err != nil {
		return nil, err
	}
	key := eff.Key
	keyLen := uint32(len(key))
	buf := make([]byte, 4+len(key)+len(protoData))
	binary.LittleEndian.PutUint32(buf[:4], keyLen)
	copy(buf[4:4+keyLen], key)
	copy(buf[4+keyLen:], protoData)
	return buf, nil
}
