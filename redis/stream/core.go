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
	"encoding/binary"
	"log/slog"
	"sort"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// timeNow is a function variable for getting current time, allowing tests to override.
var timeNow = time.Now

// --- Key helpers ---
// Stream entries live at the stream key itself (ORDERED collection).
// Related state uses null-byte-delimited sub-keys for independent mergeability.

func producerKey(stream, producer string) string { return stream + "\x01p\x01" + producer }
func groupKey(stream, group string) string       { return stream + "\x01g\x01" + group }
func pelKey(stream, group string) string         { return stream + "\x01pel\x01" + group }
func cfgKey(stream string) string                { return stream + "\x01cfg" }

// --- Snapshot helpers ---

// getStreamSnapshot retrieves and validates a stream key from the effects engine.
// Streams use ORDERED collections.
func getStreamSnapshot(cmd *shared.Command, key string) (snap *pb.ReducedEffect, tips []effects.Tip, exists bool, wrongType bool, err error) {
	snap, tips, err = cmd.Context.GetSnapshot(key)
	if err != nil {
		return nil, nil, false, false, err
	}
	if snap == nil || snap.Op == pb.EffectOp_REMOVE_OP {
		return nil, tips, false, false, nil
	}
	if snap.Collection == pb.CollectionKind_ORDERED {
		return snap, tips, true, false, nil
	}
	if snap.Collection == pb.CollectionKind_SCALAR && snap.TypeTag == pb.ValueType_TYPE_STREAM {
		return snap, tips, true, false, nil
	}
	// Legacy: accept KEYED streams that haven't been migrated yet
	if snap.Collection == pb.CollectionKind_KEYED && snap.TypeTag == pb.ValueType_TYPE_STREAM {
		return snap, tips, true, false, nil
	}
	return nil, tips, true, true, nil
}

// --- Emit helpers ---

// emitStreamInsert emits an INSERT effect for a stream entry using ORDERED collection.
// The element ID is the 16-byte big-endian encoding of the StreamID.
// Placement is PLACE_TAIL so entries append in order.
func emitStreamInsert(cmd *shared.Command, key string, id shared.StreamID, fields [][]byte) error {
	elementID := encodeStreamElementID(id)
	raw := encodeStreamFields(fields)
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_ORDERED,
			Placement:  pb.Placement_PLACE_TAIL,
			Id:         elementID,
			Value:      &pb.DataEffect_Raw{Raw: raw},
		}},
	})
}

// emitStreamRemove emits a REMOVE effect for a stream entry.
func emitStreamRemove(cmd *shared.Command, key string, id shared.StreamID) error {
	elementID := encodeStreamElementID(id)
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_ORDERED,
			Id:         elementID,
		}},
	})
}

// emitStreamTypeTag emits a MetaEffect to mark a key as TYPE_STREAM.
func emitStreamTypeTag(cmd *shared.Command, key string) error {
	return cmd.Context.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_STREAM}},
	})
}

// --- Encoding helpers (kept from original) ---

// encodeStreamElementID encodes a StreamID as a 16-byte big-endian value for lexicographic ordering.
func encodeStreamElementID(id shared.StreamID) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], id.Ms)
	binary.BigEndian.PutUint64(buf[8:16], id.Seq)
	return buf
}

// decodeStreamElementID decodes a 16-byte big-endian element ID back to a StreamID.
func decodeStreamElementID(id []byte) shared.StreamID {
	if len(id) != 16 {
		return shared.StreamID{}
	}
	return shared.StreamID{
		Ms:  binary.BigEndian.Uint64(id[0:8]),
		Seq: binary.BigEndian.Uint64(id[8:16]),
	}
}

// encodeStreamFields encodes alternating field key-value pairs into raw bytes.
// Format: [count:4][len1:4][data1][len2:4][data2]...
func encodeStreamFields(fields [][]byte) []byte {
	size := 4 // count
	for _, f := range fields {
		size += 4 + len(f)
	}
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(fields)))
	off := 4
	for _, f := range fields {
		binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(f)))
		off += 4
		copy(buf[off:off+len(f)], f)
		off += len(f)
	}
	return buf
}

// decodeStreamFields decodes raw bytes back to alternating field key-value pairs.
func decodeStreamFields(raw []byte) [][]byte {
	if len(raw) < 4 {
		return nil
	}
	count := int(binary.LittleEndian.Uint32(raw[0:4]))
	if count == 0 {
		return nil
	}
	fields := make([][]byte, count)
	off := 4
	for i := 0; i < count && off+4 <= len(raw); i++ {
		fLen := int(binary.LittleEndian.Uint32(raw[off : off+4]))
		off += 4
		if off+fLen > len(raw) {
			break
		}
		fields[i] = raw[off : off+fLen]
		off += fLen
	}
	return fields
}

// --- Snapshot → entries conversion ---

// snapshotToEntries converts a ReducedEffect snapshot to a sorted slice of StreamEntry.
// For ORDERED collections, entries are in OrderedElements (already in order).
// For legacy KEYED collections, entries are in NetAdds (need sorting).
func snapshotToEntries(snap *pb.ReducedEffect) []*shared.StreamEntry {
	if snap == nil {
		return nil
	}

	// ORDERED path: use OrderedElements directly
	if snap.Collection == pb.CollectionKind_ORDERED && len(snap.OrderedElements) > 0 {
		entries := make([]*shared.StreamEntry, 0, len(snap.OrderedElements))
		for _, elem := range snap.OrderedElements {
			if len(elem.Data.Id) != 16 {
				continue
			}
			id := decodeStreamElementID(elem.Data.Id)
			fields := decodeStreamFields(elem.Data.Decompress())
			entries = append(entries, &shared.StreamEntry{
				ID:     id,
				Fields: fields,
			})
		}
		return entries
	}

	// Legacy KEYED path: iterate NetAdds
	if snap.NetAdds == nil {
		return nil
	}

	entries := make([]*shared.StreamEntry, 0, len(snap.NetAdds))
	for elemID, elem := range snap.NetAdds {
		// Skip metadata element
		if elemID == "__meta__" {
			continue
		}
		if len(elemID) != 16 {
			continue
		}
		id := decodeStreamElementID([]byte(elemID))
		fields := decodeStreamFields(elem.Data.Decompress())
		entries = append(entries, &shared.StreamEntry{
			ID:     id,
			Fields: fields,
		})
	}

	// Sort by StreamID
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].ID.Compare(entries[j].ID) < 0
	})

	return entries
}

// --- Stream metadata (derived from entries, no more __meta__) ---

// streamMetadata extracts stream metadata from a snapshot.
// For ORDERED: LastID from tail of OrderedElements, entriesAdded from element count.
// For legacy KEYED: reads from __meta__ element.
func streamMetadata(snap *pb.ReducedEffect) (lastID shared.StreamID, entriesAdded int64, maxDeletedID shared.StreamID, found bool) {
	if snap == nil {
		return
	}

	// Legacy KEYED path: check for __meta__ element
	if snap.Collection == pb.CollectionKind_KEYED && snap.NetAdds != nil {
		if meta, ok := snap.NetAdds["__meta__"]; ok {
			raw := meta.Data.Decompress()
			if len(raw) >= 40 {
				lastID.Ms = binary.BigEndian.Uint64(raw[0:8])
				lastID.Seq = binary.BigEndian.Uint64(raw[8:16])
				entriesAdded = int64(binary.LittleEndian.Uint64(raw[16:24]))
				maxDeletedID.Ms = binary.BigEndian.Uint64(raw[24:32])
				maxDeletedID.Seq = binary.BigEndian.Uint64(raw[32:40])
				found = true
				return
			}
		}
	}

	// ORDERED path: derive from OrderedElements and NetRemoves
	if snap.Collection == pb.CollectionKind_ORDERED {
		if len(snap.OrderedElements) > 0 {
			last := snap.OrderedElements[len(snap.OrderedElements)-1]
			if len(last.Data.Id) == 16 {
				lastID = decodeStreamElementID(last.Data.Id)
			}
			found = true
		}
		// entries-added defaults to current count but may be overridden by cfg key
		entriesAdded = int64(len(snap.OrderedElements))
		// MaxDeletedID: derive from NetRemoves (the max removed ID)
		for elemID := range snap.NetRemoves {
			if len(elemID) == 16 {
				id := decodeStreamElementID([]byte(elemID))
				if id.Compare(maxDeletedID) > 0 {
					maxDeletedID = id
				}
			}
		}
		// lastID should be at least as large as maxDeletedID
		// (e.g., XSETID inserts a sentinel and removes it)
		if maxDeletedID.Compare(lastID) > 0 {
			lastID = maxDeletedID
			found = true
		}
		if found || len(snap.NetRemoves) > 0 {
			found = true
		}
		return
	}

	return
}

// streamLen returns the entry count from a snapshot.
func streamLen(snap *pb.ReducedEffect) int {
	if snap == nil {
		return 0
	}
	if snap.Collection == pb.CollectionKind_ORDERED {
		return len(snap.OrderedElements)
	}
	// Legacy KEYED path
	if snap.NetAdds == nil {
		return 0
	}
	n := len(snap.NetAdds)
	if _, hasMeta := snap.NetAdds["__meta__"]; hasMeta {
		n--
	}
	return n
}

// streamFirstID returns the first (smallest) entry ID from sorted entries.
func streamFirstID(entries []*shared.StreamEntry) shared.StreamID {
	if len(entries) == 0 {
		return shared.StreamID{}
	}
	return entries[0].ID
}

// streamLastID returns the last (largest) entry ID from sorted entries.
func streamLastID(entries []*shared.StreamEntry) shared.StreamID {
	if len(entries) == 0 {
		return shared.StreamID{}
	}
	return entries[len(entries)-1].ID
}

// --- ID generation ---

// generateStreamID generates a new stream ID based on the snapshot's metadata.
func generateStreamID(snap *pb.ReducedEffect) shared.StreamID {
	lastID, _, _, _ := streamMetadata(snap)
	// If no metadata, derive lastID from entries
	if lastID.IsZero() {
		entries := snapshotToEntries(snap)
		lastID = streamLastID(entries)
	}

	now := uint64(timeNow().UnixMilli())
	if now > lastID.Ms {
		return shared.StreamID{Ms: now, Seq: 0}
	}
	if now == lastID.Ms {
		if lastID.Seq == ^uint64(0) {
			// Sequence overflow — increment ms
			return shared.StreamID{Ms: now + 1, Seq: 0}
		}
		return shared.StreamID{Ms: now, Seq: lastID.Seq + 1}
	}
	// Clock went backwards or last entry has future timestamp
	if lastID.Seq == ^uint64(0) {
		// Sequence overflow — increment ms
		return shared.StreamID{Ms: lastID.Ms + 1, Seq: 0}
	}
	return shared.StreamID{Ms: lastID.Ms, Seq: lastID.Seq + 1}
}

// generateStreamIDWithMs generates a new stream ID with a specific ms timestamp.
func generateStreamIDWithMs(snap *pb.ReducedEffect, ms uint64) (shared.StreamID, error) {
	lastID, _, _, _ := streamMetadata(snap)
	if lastID.IsZero() {
		entries := snapshotToEntries(snap)
		lastID = streamLastID(entries)
	}

	if ms > lastID.Ms {
		return shared.StreamID{Ms: ms, Seq: 0}, nil
	}
	if ms == lastID.Ms {
		return shared.StreamID{Ms: ms, Seq: lastID.Seq + 1}, nil
	}
	return shared.StreamID{}, shared.ErrStreamIDTooSmall
}

// --- Range helpers ---

// rangeEntries filters entries by ID range with optional count limit.
func rangeEntries(entries []*shared.StreamEntry, start, end shared.StreamID, startExcl, endExcl bool, count int) []*shared.StreamEntry {
	var result []*shared.StreamEntry
	for _, e := range entries {
		cmpStart := e.ID.Compare(start)
		if startExcl {
			if cmpStart <= 0 {
				continue
			}
		} else {
			if cmpStart < 0 {
				continue
			}
		}
		cmpEnd := e.ID.Compare(end)
		if endExcl {
			if cmpEnd >= 0 {
				continue
			}
		} else {
			if cmpEnd > 0 {
				break
			}
		}
		result = append(result, e)
		if count > 0 && len(result) >= count {
			break
		}
	}
	return result
}

// rangeEntriesReverse filters entries by ID range in reverse with optional count limit.
func rangeEntriesReverse(entries []*shared.StreamEntry, start, end shared.StreamID, startExcl, endExcl bool, count int) []*shared.StreamEntry {
	var result []*shared.StreamEntry
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		cmpEnd := e.ID.Compare(end)
		if endExcl {
			if cmpEnd >= 0 {
				continue
			}
		} else {
			if cmpEnd > 0 {
				continue
			}
		}
		cmpStart := e.ID.Compare(start)
		if startExcl {
			if cmpStart <= 0 {
				continue
			}
		} else {
			if cmpStart < 0 {
				break
			}
		}
		result = append(result, e)
		if count > 0 && len(result) >= count {
			break
		}
	}
	return result
}

// --- Consumer group emit helpers ---
// Groups and consumers are stored in the groupKey as a KEYED collection.
// PEL entries are stored in the pelKey as a KEYED collection.

// emitGroupInsert inserts/updates a group in the group key for a stream.
func emitGroupInsert(cmd *shared.Command, streamKey, groupName string, metaRaw []byte) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(groupKey(streamKey, groupName)),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte("__group__"),
			Value:      &pb.DataEffect_Raw{Raw: metaRaw},
		}},
	})
}

// emitGroupRemove removes a group by removing its entire key.
func emitGroupRemove(cmd *shared.Command, streamKey, groupName string) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(groupKey(streamKey, groupName)),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_SCALAR, // full key delete, not element remove
		}},
	})
}

// encodeGroupMeta encodes consumer group metadata.
// Format: LastDeliveredID (16 bytes) + EntriesRead (8 bytes) + CreatedAt (8 bytes) = 32 bytes
func encodeGroupMeta(lastDeliveredID shared.StreamID, entriesRead int64, createdAt int64) []byte {
	buf := make([]byte, 32)
	binary.BigEndian.PutUint64(buf[0:8], lastDeliveredID.Ms)
	binary.BigEndian.PutUint64(buf[8:16], lastDeliveredID.Seq)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(entriesRead))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(createdAt))
	return buf
}

// decodeGroupMeta decodes consumer group metadata.
func decodeGroupMeta(raw []byte) (lastDeliveredID shared.StreamID, entriesRead int64, createdAt int64) {
	if len(raw) < 32 {
		return
	}
	lastDeliveredID.Ms = binary.BigEndian.Uint64(raw[0:8])
	lastDeliveredID.Seq = binary.BigEndian.Uint64(raw[8:16])
	entriesRead = int64(binary.LittleEndian.Uint64(raw[16:24]))
	createdAt = int64(binary.LittleEndian.Uint64(raw[24:32]))
	return
}

// emitConsumerInsert inserts a consumer into the group key with consumer name as element ID.
func emitConsumerInsert(cmd *shared.Command, streamKey, group, consumer string, metaRaw []byte) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(groupKey(streamKey, group)),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte("c\x00" + consumer),
			Value:      &pb.DataEffect_Raw{Raw: metaRaw},
		}},
	})
}

// emitConsumerRemove removes a consumer from the group key.
func emitConsumerRemove(cmd *shared.Command, streamKey, group, consumer string) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(groupKey(streamKey, group)),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte("c\x00" + consumer),
		}},
	})
}

// encodeConsumerMeta encodes consumer metadata.
// Format: SeenTime (8 bytes) + ActiveTime (8 bytes) = 16 bytes
func encodeConsumerMeta(seenTime, activeTime int64) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(seenTime))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(activeTime))
	return buf
}

// decodeConsumerMeta decodes consumer metadata.
func decodeConsumerMeta(raw []byte) (seenTime, activeTime int64) {
	if len(raw) < 16 {
		return
	}
	seenTime = int64(binary.LittleEndian.Uint64(raw[0:8]))
	activeTime = int64(binary.LittleEndian.Uint64(raw[8:16]))
	return
}

// --- PEL helpers ---

// emitPELInsert inserts or updates a PEL entry.
func emitPELInsert(cmd *shared.Command, streamKey, group string, id shared.StreamID, pelRaw []byte) error {
	elementID := encodeStreamElementID(id)
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(pelKey(streamKey, group)),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         elementID,
			Value:      &pb.DataEffect_Raw{Raw: pelRaw},
		}},
	})
}

// emitPELKeyRemove deletes the entire PEL key for a group (used by XGROUP DESTROY).
func emitPELKeyRemove(cmd *shared.Command, streamKey, group string) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(pelKey(streamKey, group)),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_SCALAR, // full key delete
		}},
	})
}

// emitPELRemove removes a PEL entry.
func emitPELRemove(cmd *shared.Command, streamKey, group string, id shared.StreamID) error {
	elementID := encodeStreamElementID(id)
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(pelKey(streamKey, group)),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_KEYED,
			Id:         elementID,
		}},
	})
}

// encodePELEntry encodes a PEL entry.
// Format: consumerNameLen (4 bytes) + consumerName + deliveryTime (8 bytes) + deliveryCount (8 bytes)
func encodePELEntry(consumer string, deliveryTime, deliveryCount int64) []byte {
	buf := make([]byte, 4+len(consumer)+16)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(consumer)))
	copy(buf[4:4+len(consumer)], consumer)
	off := 4 + len(consumer)
	binary.LittleEndian.PutUint64(buf[off:off+8], uint64(deliveryTime))
	binary.LittleEndian.PutUint64(buf[off+8:off+16], uint64(deliveryCount))
	return buf
}

// decodePELEntry decodes a PEL entry.
func decodePELEntry(raw []byte) (consumer string, deliveryTime, deliveryCount int64) {
	if len(raw) < 4 {
		return
	}
	nameLen := int(binary.LittleEndian.Uint32(raw[0:4]))
	if len(raw) < 4+nameLen+16 {
		return
	}
	consumer = string(raw[4 : 4+nameLen])
	off := 4 + nameLen
	deliveryTime = int64(binary.LittleEndian.Uint64(raw[off : off+8]))
	deliveryCount = int64(binary.LittleEndian.Uint64(raw[off+8 : off+16]))
	return
}

// --- Group/PEL snapshot helpers ---

// getGroupSnapshot retrieves the group metadata snapshot for a stream+group.
// The group key stores: "__group__" element for group meta, "c\x00{consumer}" for consumers.
func getGroupSnapshot(cmd *shared.Command, streamKey, group string) (snap *pb.ReducedEffect, tips []effects.Tip, err error) {
	snap, tips, err = cmd.Context.GetSnapshot(groupKey(streamKey, group))
	return
}

// getPELSnapshot retrieves the PEL snapshot for a group.
func getPELSnapshot(cmd *shared.Command, streamKey, group string) (snap *pb.ReducedEffect, tips []effects.Tip, err error) {
	snap, tips, err = cmd.Context.GetSnapshot(pelKey(streamKey, group))
	return
}

// groupExists checks if a group exists by looking for the "__group__" element.
func groupExists(cmd *shared.Command, streamKey, groupName string) bool {
	snap, _, err := cmd.Context.GetSnapshot(groupKey(streamKey, groupName))
	if err != nil || snap == nil || snap.NetAdds == nil {
		return false
	}
	_, exists := snap.NetAdds["__group__"]
	return exists
}

// getGroupMeta retrieves a group's metadata from the group key snapshot.
func getGroupMeta(snap *pb.ReducedEffect) (lastDeliveredID shared.StreamID, entriesRead int64, createdAt int64, found bool) {
	if snap == nil || snap.NetAdds == nil {
		return
	}
	elem, ok := snap.NetAdds["__group__"]
	if !ok {
		return
	}
	lastDeliveredID, entriesRead, createdAt = decodeGroupMeta(elem.Data.Decompress())
	found = true
	return
}

// getConsumersFromGroup returns consumer metadata from a group snapshot.
// Consumers are stored with "c\x00{name}" keys.
func getConsumersFromGroup(snap *pb.ReducedEffect) map[string]*pb.ReducedElement {
	if snap == nil || snap.NetAdds == nil {
		return nil
	}
	consumers := make(map[string]*pb.ReducedElement)
	for key, elem := range snap.NetAdds {
		if len(key) > 2 && key[0] == 'c' && key[1] == '\x00' {
			consumers[key[2:]] = elem
		}
	}
	return consumers
}

// listGroups returns a list of group names for a stream by scanning for group keys.
// Since groups are now stored at individual keys, we need to enumerate them.
// This function checks known group names from the caller context.
func listGroupNames(cmd *shared.Command, streamKey string) []string {
	// Groups are stored at {stream}\0g\0{group} keys.
	// We can't enumerate keys from the effects engine easily, so we use
	// a registry key that tracks group names.
	registryKey := streamKey + "\x01g\x01__registry__"
	snap, _, err := cmd.Context.GetSnapshot(registryKey)
	if err != nil || snap == nil || snap.NetAdds == nil {
		return nil
	}
	names := make([]string, 0, len(snap.NetAdds))
	for name := range snap.NetAdds {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// emitGroupRegistryInsert adds a group name to the registry.
func emitGroupRegistryInsert(cmd *shared.Command, streamKey, groupName string) error {
	registryKey := streamKey + "\x01g\x01__registry__"
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(registryKey),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(groupName),
			Value:      &pb.DataEffect_Raw{Raw: []byte{1}},
		}},
	})
}

// emitGroupRegistryRemove removes a group name from the registry.
func emitGroupRegistryRemove(cmd *shared.Command, streamKey, groupName string) error {
	registryKey := streamKey + "\x01g\x01__registry__"
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(registryKey),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(groupName),
		}},
	})
}

// pelToEntries converts PEL snapshot to sorted PendingEntry list.
func pelToEntries(pelSnap *pb.ReducedEffect) []*shared.PendingEntry {
	if pelSnap == nil || pelSnap.NetAdds == nil {
		return nil
	}

	entries := make([]*shared.PendingEntry, 0, len(pelSnap.NetAdds))
	for elemID, elem := range pelSnap.NetAdds {
		if len(elemID) != 16 {
			continue
		}
		id := decodeStreamElementID([]byte(elemID))
		consumer, deliveryTime, deliveryCount := decodePELEntry(elem.Data.Decompress())
		entries = append(entries, &shared.PendingEntry{
			ID:            id,
			Consumer:      consumer,
			DeliveryTime:  deliveryTime,
			DeliveryCount: deliveryCount,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].ID.Compare(entries[j].ID) < 0
	})

	return entries
}

// consumerPendingCount counts PEL entries for a specific consumer.
func consumerPendingCount(pelSnap *pb.ReducedEffect, consumer string) int64 {
	if pelSnap == nil || pelSnap.NetAdds == nil {
		return 0
	}
	var count int64
	for _, elem := range pelSnap.NetAdds {
		c, _, _ := decodePELEntry(elem.Data.Decompress())
		if c == consumer {
			count++
		}
	}
	return count
}

// --- Producer dedup helpers ---

// getStreamEntriesAdded reads the entries-added counter from the cfg key.
// Returns -1 if not explicitly set (caller should fall back to element count).
func getStreamEntriesAdded(cmd *shared.Command, streamKey string) int64 {
	snap, _, err := cmd.Context.GetSnapshot(cfgKey(streamKey))
	if err != nil || snap == nil || snap.NetAdds == nil {
		return -1
	}
	if elem, ok := snap.NetAdds["entries-added"]; ok {
		raw := elem.Data.Decompress()
		if len(raw) == 8 {
			return int64(binary.LittleEndian.Uint64(raw))
		}
	}
	return -1
}

// incrementStreamEntriesAdded increments the stream's entries-added counter.
func incrementStreamEntriesAdded(cmd *shared.Command, streamKey string) error {
	current := max(getStreamEntriesAdded(cmd, streamKey), 0)
	return emitCfgSet(cmd, streamKey, "entries-added", current+1)
}

// setStreamEntriesAdded sets the stream's entries-added counter to a specific value.
func setStreamEntriesAdded(cmd *shared.Command, streamKey string, value int64) error {
	return emitCfgSet(cmd, streamKey, "entries-added", value)
}

// getIdmpGeneration reads the current IDMP generation from the cfg key.
func getIdmpGeneration(cmd *shared.Command, streamKey string) int64 {
	snap, _, err := cmd.Context.GetSnapshot(cfgKey(streamKey))
	if err != nil || snap == nil || snap.NetAdds == nil {
		return 0
	}
	if elem, ok := snap.NetAdds["idmp-generation"]; ok {
		raw := elem.Data.Decompress()
		if len(raw) == 8 {
			return int64(binary.LittleEndian.Uint64(raw))
		}
	}
	return 0
}

// emitProducerSeq records a producer's sequence number → stream entry ID mapping.
func emitProducerSeq(cmd *shared.Command, streamKey, producer, seq string, entryID shared.StreamID) error {
	gen := getIdmpGeneration(cmd, streamKey)
	_, maxsize, _ := getStreamConfig(cmd, streamKey)
	return emitProducerSeqWithConfig(cmd, streamKey, producer, seq, entryID, gen, maxsize)
}

// emitProducerSeqWithConfig records a producer's sequence number with pre-read config values.
// The stored value is 32 bytes: 16 bytes StreamID + 8 bytes timestamp (ms) + 8 bytes generation.
func emitProducerSeqWithConfig(cmd *shared.Command, streamKey, producer, seq string, entryID shared.StreamID, gen int64, maxsize int64) error {
	idRaw := encodeStreamElementID(entryID)
	raw := make([]byte, 32)
	copy(raw, idRaw)
	binary.LittleEndian.PutUint64(raw[16:], uint64(timeNow().UnixMilli()))
	binary.LittleEndian.PutUint64(raw[24:], uint64(gen))

	pKey := producerKey(streamKey, producer)

	if err := cmd.Context.Emit(&pb.Effect{
		Key: []byte(pKey),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(seq),
			Value:      &pb.DataEffect_Raw{Raw: raw},
		}},
	}); err != nil {
		return err
	}

	// Enforce IDMP-MAXSIZE: evict oldest current-generation entries if over limit
	if maxsize > 0 {
		snap, _, err := cmd.Context.GetSnapshot(pKey)
		if err == nil && snap != nil && snap.NetAdds != nil {
			// Collect only current-generation entries
			type entry struct {
				seq     string
				entryID shared.StreamID
			}
			entries := make([]entry, 0, len(snap.NetAdds))
			for s, elem := range snap.NetAdds {
				if s == seq {
					continue // skip the entry we just inserted
				}
				r := elem.Data.Decompress()
				if len(r) >= 32 {
					entryGen := int64(binary.LittleEndian.Uint64(r[24:32]))
					if entryGen != gen {
						continue // skip old-generation entries
					}
				} else if gen != 0 {
					continue // old format, skip if current gen > 0
				}
				var eid shared.StreamID
				if len(r) >= 16 {
					eid = decodeStreamElementID(r[:16])
				}
				entries = append(entries, entry{seq: s, entryID: eid})
			}
			if int64(len(entries)) >= maxsize {
				// Sort by stream entry ID (monotonically increasing)
				sort.Slice(entries, func(i, j int) bool {
					return entries[i].entryID.Compare(entries[j].entryID) < 0
				})
				// Evict oldest entries to make room (keep maxsize-1 to fit the new one)
				toEvict := int64(len(entries)) - maxsize + 1
				for i := int64(0); i < toEvict && i < int64(len(entries)); i++ {
					if err := cmd.Context.Emit(&pb.Effect{
						Key: []byte(pKey),
						Kind: &pb.Effect_Data{Data: &pb.DataEffect{
							Op:         pb.EffectOp_REMOVE_OP,
							Collection: pb.CollectionKind_KEYED,
							Id:         []byte(entries[i].seq),
						}},
					}); err != nil {
						slog.Error("stream MAXLEN: REMOVE_OP emit failed", "key", pKey, "error", err)
					}
				}
			}
		}
	}

	return emitProducerRegistryInsert(cmd, streamKey, producer)
}

// emitProducerRegistryInsert adds a producer name to the registry.
func emitProducerRegistryInsert(cmd *shared.Command, streamKey, producer string) error {
	registryKey := streamKey + "\x01p\x01__registry__"
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(registryKey),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(producer),
			Value:      &pb.DataEffect_Raw{Raw: []byte{1}},
		}},
	})
}

// listProducerNames returns all registered producer names for a stream.
func listProducerNames(cmd *shared.Command, streamKey string) []string {
	registryKey := streamKey + "\x01p\x01__registry__"
	snap, _, err := cmd.Context.GetSnapshot(registryKey)
	if err != nil || snap == nil || snap.NetAdds == nil {
		return nil
	}
	names := make([]string, 0, len(snap.NetAdds))
	for name := range snap.NetAdds {
		names = append(names, name)
	}
	return names
}

// lookupProducerSeq checks if a sequence number already exists for a producer.
// Respects IDMP-DURATION config: entries older than the duration are treated as expired.
func lookupProducerSeq(cmd *shared.Command, streamKey, producer, seq string) (shared.StreamID, bool) {
	gen := getIdmpGeneration(cmd, streamKey)
	duration, _, durationExplicit := getStreamConfig(cmd, streamKey)
	if !durationExplicit {
		duration = 0
	}
	return lookupProducerSeqWithConfig(cmd, streamKey, producer, seq, gen, duration)
}

func lookupProducerSeqWithConfig(cmd *shared.Command, streamKey, producer, seq string, gen int64, duration int64) (shared.StreamID, bool) {
	snap, _, err := cmd.Context.GetSnapshot(producerKey(streamKey, producer))
	if err != nil || snap == nil || snap.NetAdds == nil {
		return shared.StreamID{}, false
	}
	entry, exists := snap.NetAdds[seq]
	if !exists {
		return shared.StreamID{}, false
	}
	raw := entry.Data.Decompress()
	// Support both old 16-byte format and new 24-byte format
	if len(raw) < 16 {
		return shared.StreamID{}, false
	}
	id := decodeStreamElementID(raw[:16])

	// Check generation — entries from old generations are stale
	if len(raw) >= 32 {
		entryGen := int64(binary.LittleEndian.Uint64(raw[24:32]))
		if entryGen != gen {
			return shared.StreamID{}, false // stale generation
		}
	}

	// Check expiry if we have timestamp and config
	if len(raw) >= 24 {
		storedAtMs := int64(binary.LittleEndian.Uint64(raw[16:24]))
		if duration > 0 && timeNow().UnixMilli()-storedAtMs > duration*1000 {
			return shared.StreamID{}, false // expired
		}
	}

	return id, true
}

// --- Stream IDMP config helpers ---

// emitCfgSet stores an IDMP config value for a stream.
func emitCfgSet(cmd *shared.Command, streamKey, field string, value int64) error {
	raw := make([]byte, 8)
	binary.LittleEndian.PutUint64(raw, uint64(value))
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(cfgKey(streamKey)),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(field),
			Value:      &pb.DataEffect_Raw{Raw: raw},
		}},
	})
}

// getStreamConfig retrieves IDMP config (duration in seconds, maxsize) for a stream.
// Defaults: duration=100, maxsize=100.
// durationExplicit is true only if duration was explicitly set via XCFGSET.
func getStreamConfig(cmd *shared.Command, streamKey string) (duration int64, maxsize int64, durationExplicit bool) {
	duration = 100
	maxsize = 100
	snap, _, err := cmd.Context.GetSnapshot(cfgKey(streamKey))
	if err != nil || snap == nil || snap.NetAdds == nil {
		return
	}
	if elem, ok := snap.NetAdds["idmp-duration"]; ok {
		raw := elem.Data.Decompress()
		if len(raw) == 8 {
			duration = int64(binary.LittleEndian.Uint64(raw))
			durationExplicit = true
		}
	}
	if elem, ok := snap.NetAdds["idmp-maxsize"]; ok {
		raw := elem.Data.Decompress()
		if len(raw) == 8 {
			maxsize = int64(binary.LittleEndian.Uint64(raw))
		}
	}
	return
}

// getIidsAdded retrieves the lifetime iids-added counter for a stream.
func getIidsAdded(cmd *shared.Command, streamKey string) int64 {
	snap, _, err := cmd.Context.GetSnapshot(cfgKey(streamKey))
	if err != nil || snap == nil || snap.NetAdds == nil {
		return 0
	}
	if elem, ok := snap.NetAdds["iids-added"]; ok {
		raw := elem.Data.Decompress()
		if len(raw) == 8 {
			return int64(binary.LittleEndian.Uint64(raw))
		}
	}
	return 0
}

// incrementIidsAdded increments the lifetime iids-added counter.
func incrementIidsAdded(cmd *shared.Command, streamKey string) error {
	current := getIidsAdded(cmd, streamKey)
	return emitCfgSet(cmd, streamKey, "iids-added", current+1)
}

// getIidsDuplicates retrieves the lifetime iids-duplicates counter.
func getIidsDuplicates(cmd *shared.Command, streamKey string) int64 {
	snap, _, err := cmd.Context.GetSnapshot(cfgKey(streamKey))
	if err != nil || snap == nil || snap.NetAdds == nil {
		return 0
	}
	if elem, ok := snap.NetAdds["iids-duplicates"]; ok {
		raw := elem.Data.Decompress()
		if len(raw) == 8 {
			return int64(binary.LittleEndian.Uint64(raw))
		}
	}
	return 0
}

// incrementIidsDuplicates increments the lifetime iids-duplicates counter.
func incrementIidsDuplicates(cmd *shared.Command, streamKey string) error {
	current := getIidsDuplicates(cmd, streamKey)
	return emitCfgSet(cmd, streamKey, "iids-duplicates", current+1)
}

// clearAllProducerKeys invalidates all IDMP history by incrementing the generation counter.
// Old entries remain in producer keys but are ignored during lookup (generation mismatch).
func clearAllProducerKeys(cmd *shared.Command, streamKey string) {
	gen := getIdmpGeneration(cmd, streamKey)
	emitCfgSet(cmd, streamKey, "idmp-generation", gen+1)
	// Also reset iids-added counter since history is cleared
	// (iids-added is a lifetime counter, but generation change means fresh start)
}

// emitProducerRegistryRemove removes a producer from the registry.
func emitProducerRegistryRemove(cmd *shared.Command, streamKey, producer string) error {
	registryKey := streamKey + "\x01p\x01__registry__"
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(registryKey),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(producer),
		}},
	})
}
