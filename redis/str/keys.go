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

package str

import (
	"bytes"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
	"github.com/zeebo/xxh3"
)

// getAnySnapshot returns the snapshot for any key type.
// Used by type-agnostic commands (EXISTS, TYPE, TTL, etc.)
func getAnySnapshot(cmd *shared.Command, key string) (snap *pb.ReducedEffect, err error) {
	snap, _, err = cmd.Context.GetSnapshot(key)
	if snap != nil && snap.Op == pb.EffectOp_REMOVE_OP {
		return nil, err
	}
	return
}

// getAnySnapshotWithTips returns the snapshot and tips for any key type.
// Used by type-agnostic write commands (EXPIRE, RENAME, etc.) that need tips for transactions.
func getAnySnapshotWithTips(cmd *shared.Command, key string) (snap *pb.ReducedEffect, tips []effects.Tip, err error) {
	snap, tips, err = cmd.Context.GetSnapshot(key)
	if err != nil {
		return nil, nil, err
	}
	if snap != nil && snap.Op == pb.EffectOp_REMOVE_OP {
		return nil, tips, nil
	}
	return snap, tips, nil
}

// emitSnapshotAtKey re-emits a full snapshot (any type) at a new key.
// Used by RENAME, RENAMENX, COPY, MOVE to recreate a key's value at a different key.
func emitSnapshotAtKey(cmd *shared.Command, destKey string, snap *pb.ReducedEffect, tips ...[]effects.Tip) error {
	switch snap.Collection {
	case pb.CollectionKind_SCALAR:
		if snap.Scalar != nil {
			if err := cmd.Context.Emit(&pb.Effect{
				Key: []byte(destKey),
				Kind: &pb.Effect_Data{Data: &pb.DataEffect{
					Op:         pb.EffectOp_INSERT_OP,
					Merge:      snap.Merge,
					Collection: pb.CollectionKind_SCALAR,
					Value:      snap.Scalar.Value,
				}},
			}, tips...); err != nil {
				return err
			}
		}
	case pb.CollectionKind_KEYED:
		first := true
		for id, elem := range snap.NetAdds {
			if first {
				if err := cmd.Context.Emit(&pb.Effect{
					Key: []byte(destKey),
					Kind: &pb.Effect_Data{Data: &pb.DataEffect{
						Op:         pb.EffectOp_INSERT_OP,
						Merge:      snap.Merge,
						Collection: pb.CollectionKind_KEYED,
						Id:         []byte(id),
						Value:      elem.Data.Value,
					}},
				}, tips...); err != nil {
					return err
				}
				first = false
			} else {
				if err := cmd.Context.Emit(&pb.Effect{
					Key: []byte(destKey),
					Kind: &pb.Effect_Data{Data: &pb.DataEffect{
						Op:         pb.EffectOp_INSERT_OP,
						Merge:      snap.Merge,
						Collection: pb.CollectionKind_KEYED,
						Id:         []byte(id),
						Value:      elem.Data.Value,
					}},
				}); err != nil {
					return err
				}
			}
			if elem.ExpiresAt != nil {
				if err := cmd.Context.Emit(&pb.Effect{
					Key: []byte(destKey),
					Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{
						ElementId: []byte(id),
						ExpiresAt: elem.ExpiresAt,
					}},
				}); err != nil {
					return err
				}
			}
		}
	case pb.CollectionKind_ORDERED:
		first := true
		for _, elem := range snap.OrderedElements {
			if first {
				if err := cmd.Context.Emit(&pb.Effect{
					Key: []byte(destKey),
					Kind: &pb.Effect_Data{Data: &pb.DataEffect{
						Op:         pb.EffectOp_INSERT_OP,
						Merge:      snap.Merge,
						Collection: pb.CollectionKind_ORDERED,
						Id:         elem.Data.Id,
						Value:      elem.Data.Value,
					}},
				}, tips...); err != nil {
					return err
				}
				first = false
			} else {
				if err := cmd.Context.Emit(&pb.Effect{
					Key: []byte(destKey),
					Kind: &pb.Effect_Data{Data: &pb.DataEffect{
						Op:         pb.EffectOp_INSERT_OP,
						Merge:      snap.Merge,
						Collection: pb.CollectionKind_ORDERED,
						Id:         elem.Data.Id,
						Value:      elem.Data.Value,
					}},
				}); err != nil {
					return err
				}
			}
		}
	}

	// Emit type tag
	if snap.TypeTag != pb.ValueType_TYPE_UNSPECIFIED {
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(destKey),
			Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: snap.TypeTag}},
		}); err != nil {
			return err
		}
	}

	// Emit key-level TTL
	if snap.ExpiresAt != nil {
		if err := emitStringTTL(cmd, destKey, snap.ExpiresAt); err != nil {
			return err
		}
	}

	return nil
}

// typeTagToString maps proto ValueType to Redis TYPE response strings.
func typeTagToString(tag pb.ValueType) string {
	switch tag {
	case pb.ValueType_TYPE_STRING:
		return "string"
	case pb.ValueType_TYPE_LIST:
		return "list"
	case pb.ValueType_TYPE_HASH:
		return "hash"
	case pb.ValueType_TYPE_SET:
		return "set"
	case pb.ValueType_TYPE_ZSET, pb.ValueType_TYPE_GEO:
		return "zset"
	case pb.ValueType_TYPE_STREAM:
		return "stream"
	case pb.ValueType_TYPE_HLL:
		return "string"
	case pb.ValueType_TYPE_BITMAP:
		return "string"
	default:
		return "none"
	}
}

func handleDel(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("del")
		return
	}

	keys = make([]string, len(cmd.Args))
	for i, arg := range cmd.Args {
		keys[i] = string(arg)
	}
	valid = true

	runner = func() {
		deleted := int64(0)
		for _, key := range keys {
			snap, err := getAnySnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if snap == nil {
				continue
			}
			if err := emitDeleteKey(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
			// Always attempt stream sub-key cleanup — the key might have been
			// overwritten (e.g. SET) but stream sub-keys could still exist.
			if err := cleanupStreamGroups(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
			deleted++
		}

		w.WriteInteger(deleted)
		if s := shared.GetServerStats(); s != nil {
			s.CmdDel.Add(uint64(deleted))
		}
	}
	return
}

// cleanupStreamGroups deletes all consumer group and producer sub-keys for a
// stream. Stream sub-keys use \x01-delimited key naming. A read or emit
// failure aborts the cleanup: treating an unanswerable read as "no groups"
// would leak sub-keys behind the deleted stream.
func cleanupStreamGroups(cmd *shared.Command, key string) error {
	// Clean up consumer groups
	groupRegistryKey := key + "\x01g\x01__registry__"
	registrySnap, _, err := cmd.Context.GetSnapshot(groupRegistryKey)
	if err != nil {
		return err
	}
	if registrySnap != nil && registrySnap.NetAdds != nil {
		for groupName := range registrySnap.NetAdds {
			// Delete per-group sub-keys: group metadata and PEL
			groupKeyName := key + "\x01g\x01" + groupName
			pelKeyName := key + "\x01pel\x01" + groupName
			if err := emitDeleteKey(cmd, groupKeyName); err != nil {
				return err
			}
			if err := emitDeleteKey(cmd, pelKeyName); err != nil {
				return err
			}
		}
	}
	if err := emitDeleteKey(cmd, groupRegistryKey); err != nil {
		return err
	}

	// Clean up producer dedup keys
	producerRegistryKey := key + "\x01p\x01__registry__"
	producerSnap, _, err := cmd.Context.GetSnapshot(producerRegistryKey)
	if err != nil {
		return err
	}
	if producerSnap != nil && producerSnap.NetAdds != nil {
		for producerName := range producerSnap.NetAdds {
			producerKeyName := key + "\x01p\x01" + producerName
			if err := emitDeleteKey(cmd, producerKeyName); err != nil {
				return err
			}
		}
	}
	if err := emitDeleteKey(cmd, producerRegistryKey); err != nil {
		return err
	}

	// Clean up IDMP config key (KEYED collection — remove each element)
	cfgKeyName := key + "\x01cfg"
	cfgSnap, _, err := cmd.Context.GetSnapshot(cfgKeyName)
	if err != nil {
		return err
	}
	if cfgSnap != nil && cfgSnap.NetAdds != nil {
		for elemID := range cfgSnap.NetAdds {
			if err := cmd.Context.Emit(&pb.Effect{
				Key: []byte(cfgKeyName),
				Kind: &pb.Effect_Data{Data: &pb.DataEffect{
					Op:         pb.EffectOp_REMOVE_OP,
					Collection: pb.CollectionKind_KEYED,
					Id:         []byte(elemID),
				}},
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// copyStreamGroups copies all consumer group sub-keys from srcKey to dstKey.
// A read or emit failure aborts the copy: treating an unanswerable read as
// "no groups" would silently drop the groups from the destination.
func copyStreamGroups(cmd *shared.Command, srcKey, dstKey string) error {
	srcRegistryKey := srcKey + "\x01g\x01__registry__"
	dstRegistryKey := dstKey + "\x01g\x01__registry__"
	registrySnap, _, err := cmd.Context.GetSnapshot(srcRegistryKey)
	if err != nil {
		return err
	}
	if registrySnap == nil || registrySnap.NetAdds == nil {
		return nil
	}
	// Copy the groups registry
	if err := emitSnapshotAtKey(cmd, dstRegistryKey, registrySnap); err != nil {
		return err
	}
	// Copy per-group sub-keys
	for groupName := range registrySnap.NetAdds {
		srcGroupKey := srcKey + "\x01g\x01" + groupName
		dstGroupKey := dstKey + "\x01g\x01" + groupName
		srcPelKey := srcKey + "\x01pel\x01" + groupName
		dstPelKey := dstKey + "\x01pel\x01" + groupName

		snap, _, err := cmd.Context.GetSnapshot(srcGroupKey)
		if err != nil {
			return err
		}
		if snap != nil {
			if err := emitSnapshotAtKey(cmd, dstGroupKey, snap); err != nil {
				return err
			}
		}
		pelSnap, _, err := cmd.Context.GetSnapshot(srcPelKey)
		if err != nil {
			return err
		}
		if pelSnap != nil {
			if err := emitSnapshotAtKey(cmd, dstPelKey, pelSnap); err != nil {
				return err
			}
		}
	}
	return nil
}

func handleDelEx(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	// DELEX key [IFEQ value | IFNE value | IFDEQ digest | IFDNE digest]
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("delex")
		return
	}

	key := string(cmd.Args[0])

	// Parse option - must have exactly 3 args if options present: key, condition, value
	if len(cmd.Args) != 1 && len(cmd.Args) != 3 {
		w.WriteError("ERR wrong number of arguments for 'delex' command")
		return
	}

	var optStr string
	var compareValue []byte
	if len(cmd.Args) == 3 {
		opt := shared.ToUpper(cmd.Args[1])
		optStr = string(opt)
		compareValue = cmd.Args[2]

		// Validate condition
		if optStr != "IFEQ" && optStr != "IFNE" && optStr != "IFDEQ" && optStr != "IFDNE" {
			w.WriteError("ERR Invalid condition for DELEX")
			return
		}
	}

	keys = []string{key}
	valid = true

	runner = func() {
		// No options - unconditional delete (like DEL but single key)
		if optStr == "" {
			snap, err := getAnySnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if snap == nil {
				w.WriteZero()
				return
			}
			if err := emitDeleteKey(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
			w.WriteOne()
			return
		}

		// Conditional delete — needs snapshot + transaction
		snap, tips, exists, wrongType, err := getStringSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteZero()
			return
		}
		if wrongType {
			w.WriteError("ERR DELEX with conditions only supports string values")
			return
		}

		// Validate digest format
		if (optStr == "IFDEQ" || optStr == "IFDNE") && !isValidHexDigest(compareValue) {
			w.WriteError("ERR digest must be exactly 16 hexadecimal characters")
			return
		}

		raw := snapshotToRaw(snap)
		shouldDelete := false
		switch optStr {
		case "IFEQ":
			shouldDelete = bytes.Equal(raw, compareValue)
		case "IFNE":
			shouldDelete = !bytes.Equal(raw, compareValue)
		case "IFDEQ":
			shouldDelete = digestEquals(xxh3.Hash(raw), compareValue)
		case "IFDNE":
			shouldDelete = !digestEquals(xxh3.Hash(raw), compareValue)
		}

		if !shouldDelete {
			w.WriteZero()
			return
		}

		cmd.Context.BeginTx()
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}
		if err := emitDeleteKey(cmd, key); err != nil {
			w.WriteError(err.Error())
			return
		}
		w.WriteOne()
	}
	return
}

func handleExists(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("exists")
		return
	}

	keys = make([]string, len(cmd.Args))
	for i, arg := range cmd.Args {
		keys[i] = string(arg)
	}
	valid = true

	runner = func() {
		count := int64(0)
		for _, key := range keys {
			snap, err := getAnySnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if snap != nil {
				count++
			}
		}
		w.WriteInteger(count)
	}
	return
}

func handleRename(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("rename")
		return
	}

	key := string(cmd.Args[0])
	newKey := string(cmd.Args[1])
	keys = []string{key, newKey}
	valid = true

	runner = func() {
		snap, tips, err := getAnySnapshotWithTips(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if snap == nil {
			w.WriteError("ERR no such key")
			return
		}

		if key == newKey {
			w.WriteOK()
			return
		}

		cmd.Context.BeginTx()
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}
		destSnap, destTips, err := getAnySnapshotWithTips(cmd, newKey)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if len(destTips) > 0 {
			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(newKey),
				Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
			}, destTips); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if err := emitDeleteKey(cmd, key); err != nil {
			w.WriteError(err.Error())
			return
		}
		if err := emitDeleteKey(cmd, newKey); err != nil {
			w.WriteError(err.Error())
			return
		}
		if err := emitSnapshotAtKey(cmd, newKey, snap); err != nil {
			w.WriteError(err.Error())
			return
		}
		// Move stream consumer group sub-keys from old key to new key
		if snap.TypeTag == pb.ValueType_TYPE_STREAM {
			// Clean up dest groups (in case newKey was also a stream)
			if destSnap != nil && destSnap.TypeTag == pb.ValueType_TYPE_STREAM {
				if err := cleanupStreamGroups(cmd, newKey); err != nil {
					w.WriteError(err.Error())
					return
				}
			}
			if err := copyStreamGroups(cmd, key, newKey); err != nil {
				w.WriteError(err.Error())
				return
			}
			if err := cleanupStreamGroups(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		} else if destSnap != nil && destSnap.TypeTag == pb.ValueType_TYPE_STREAM {
			// Dest was a stream but source isn't — clean up dest's groups
			if err := cleanupStreamGroups(cmd, newKey); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteOK()
	}
	return
}

func handleRenameNX(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("renamenx")
		return
	}

	key := string(cmd.Args[0])
	newKey := string(cmd.Args[1])
	keys = []string{key, newKey}
	valid = true

	runner = func() {
		snap, tips, err := getAnySnapshotWithTips(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if snap == nil {
			w.WriteError("ERR no such key")
			return
		}

		if key == newKey {
			w.WriteZero()
			return
		}

		destSnap, destTips, err := getAnySnapshotWithTips(cmd, newKey)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if destSnap != nil {
			w.WriteZero()
			return
		}

		cmd.Context.BeginTx()
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}
		if len(destTips) > 0 {
			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(newKey),
				Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
			}, destTips); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if err := emitDeleteKey(cmd, key); err != nil {
			w.WriteError(err.Error())
			return
		}
		if err := emitSnapshotAtKey(cmd, newKey, snap); err != nil {
			w.WriteError(err.Error())
			return
		}
		// Move stream consumer group sub-keys from old key to new key
		if snap.TypeTag == pb.ValueType_TYPE_STREAM {
			if err := copyStreamGroups(cmd, key, newKey); err != nil {
				w.WriteError(err.Error())
				return
			}
			if err := cleanupStreamGroups(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteOne()
	}
	return
}

// handleCopy implements the COPY command
// COPY source destination [DB destination-db] [REPLACE]
func handleCopy(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("copy")
		return
	}

	srcKey := string(cmd.Args[0])
	dstKey := string(cmd.Args[1])

	// Parse optional arguments
	destDB := db
	replace := false

	for i := 2; i < len(cmd.Args); i++ {
		arg := shared.ToUpper(cmd.Args[i])
		switch string(arg) {
		case "DB":
			if i+1 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			dbIndex, ok := shared.ParseInt64(cmd.Args[i+1])
			if !ok {
				w.WriteNotInteger()
				return
			}
			if dbIndex < 0 || dbIndex >= int64(db.Manager().NumDatabases()) {
				w.WriteError("ERR invalid DB index")
				return
			}
			destDB = db.Manager().GetDB(int(dbIndex))
			if destDB == nil {
				w.WriteError("ERR invalid DB index")
				return
			}
			i++
		case "REPLACE":
			replace = true
		default:
			w.WriteSyntaxError()
			return
		}
	}

	keys = []string{srcKey, dstKey}
	valid = true

	runner = func() {
		snap, _, err := getAnySnapshotWithTips(cmd, srcKey)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if snap == nil {
			w.WriteZero()
			return
		}

		if !replace {
			destSnap, _, err := getAnySnapshotWithTips(cmd, dstKey)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if destSnap != nil {
				w.WriteZero()
				return
			}
		}

		if replace {
			// Check dest type before deleting so we can clean up stream groups
			destSnap, _, err := getAnySnapshotWithTips(cmd, dstKey)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if err := emitDeleteKey(cmd, dstKey); err != nil {
				w.WriteError(err.Error())
				return
			}
			if destSnap != nil && destSnap.TypeTag == pb.ValueType_TYPE_STREAM {
				if err := cleanupStreamGroups(cmd, dstKey); err != nil {
					w.WriteError(err.Error())
					return
				}
			}
		}

		if err := emitSnapshotAtKey(cmd, dstKey, snap); err != nil {
			w.WriteError(err.Error())
			return
		}
		// Copy stream consumer group sub-keys to dest
		if snap.TypeTag == pb.ValueType_TYPE_STREAM {
			if err := copyStreamGroups(cmd, srcKey, dstKey); err != nil {
				w.WriteError(err.Error())
				return
			}
		}
		w.WriteOne()
	}
	return
}

// handleMove implements the MOVE command
// MOVE key db
func handleMove(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("move")
		return
	}

	key := string(cmd.Args[0])
	dbIndex, ok := shared.ParseInt64(cmd.Args[1])
	if !ok {
		w.WriteNotInteger()
		return
	}
	if dbIndex < 0 || dbIndex >= int64(db.Manager().NumDatabases()) {
		w.WriteError("ERR invalid DB index")
		return
	}

	destDB := db.Manager().GetDB(int(dbIndex))
	if destDB == nil {
		w.WriteError("ERR invalid DB index")
		return
	}

	// Can't move to the same database
	if destDB == db {
		w.WriteError("ERR source and destination objects are the same")
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, err := getAnySnapshotWithTips(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if snap == nil {
			w.WriteZero()
			return
		}

		// Check if dest key exists in dest DB (keys are namespaced by DB internally)
		destSnap, destTips, err := getAnySnapshotWithTips(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if destSnap != nil {
			w.WriteZero()
			return
		}

		cmd.Context.BeginTx()
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}
		if len(destTips) > 0 {
			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(key),
				Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
			}, destTips); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if err := emitDeleteKey(cmd, key); err != nil {
			w.WriteError(err.Error())
			return
		}
		if err := emitSnapshotAtKey(cmd, key, snap); err != nil {
			w.WriteError(err.Error())
			return
		}
		w.WriteOne()
	}
	return
}

func handleType(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("type")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, err := getAnySnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if snap == nil {
			w.WriteSimpleString("none")
			return
		}
		w.WriteSimpleString(typeTagToString(snap.TypeTag))
	}
	return
}
