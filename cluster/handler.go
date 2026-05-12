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
	pb "github.com/swytchdb/swytch/cluster/proto"
)

// EffectHandler is called when a remote notification or data arrives.
type EffectHandler interface {
	// HandleOffsetNotify processes a remote effect notification.
	// Returns all NACKs generated (one per diverged key). The caller
	// sends these as the ReplicateTo response so the originator can
	// evaluate fork-choice across all keys.
	HandleOffsetNotify(notify *pb.OffsetNotify) ([]*pb.NackNotify, error)
	// HandleNack processes an enriched NACK from a remote peer.
	HandleNack(nack *pb.NackNotify) error
}

// ForwardHandler executes forwarded transactions received from other nodes
// via the adaptive serialization mechanism (§5). The serialization leader
// receives the full transaction, executes it locally, and returns the
// raw RESP wire bytes.
type ForwardHandler interface {
	HandleForwardedTransaction(tx *pb.ForwardedTransaction) *pb.ForwardedResponse
}

// LogReader reads effect data from the local log by offset.
// The redis package provides the concrete implementation at startup.
//
// Wire format: ReadEffect returns [4 bytes keyLen (little-endian)][key][effectData].
// This allows the receiver to reconstruct the full log entry including the key,
// which is stored separately from the effect data in the log.
type LogReader interface {
	// ReadEffect reads effect bytes from the local log at the given offset.
	// Returns the data in wire format: [4-byte LE keyLen][key bytes][effect data].
	ReadEffect(offset *pb.EffectRef) ([]byte, error)
}
