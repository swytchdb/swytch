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
	"sort"
	"strconv"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/swytch/redis/shared"
)

// Consumer group command implementations

// handleXGroup handles XGROUP subcommands
func handleXGroup(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("xgroup")
		return
	}

	subCmd := string(shared.ToUpper(cmd.Args[0]))

	switch subCmd {
	case "CREATE":
		return handleXGroupCreate(cmd, w, db)
	case "DESTROY":
		return handleXGroupDestroy(cmd, w, db)
	case "CREATECONSUMER":
		return handleXGroupCreateConsumer(cmd, w, db)
	case "DELCONSUMER":
		return handleXGroupDelConsumer(cmd, w, db)
	case "SETID":
		return handleXGroupSetID(cmd, w, db)
	case "HELP":
		if len(cmd.Args) > 1 {
			w.WriteError("ERR wrong number of arguments for 'xgroup|help' command")
			return
		}
		valid = true
		runner = func() {
			handleXGroupHelp(w)
		}
		return
	default:
		w.WriteError("ERR unknown subcommand '" + subCmd + "'. Try XGROUP HELP.")
		return
	}
}

// handleXGroupCreate handles XGROUP CREATE key group <id|$> [MKSTREAM] [ENTRIESREAD entries-read]
func handleXGroupCreate(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("xgroup|create")
		return
	}

	key := string(cmd.Args[1])
	groupName := string(cmd.Args[2])
	idStr := string(cmd.Args[3])
	keys = []string{key}

	var mkStream bool
	var entriesRead int64 = -1

	for i := 4; i < len(cmd.Args); i++ {
		opt := string(shared.ToUpper(cmd.Args[i]))
		switch opt {
		case "MKSTREAM":
			mkStream = true
		case "ENTRIESREAD":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			er, err := strconv.ParseInt(string(cmd.Args[i]), 10, 64)
			if err != nil {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			if er < -1 {
				w.WriteError("ERR value for ENTRIESREAD must be positive or -1")
				return
			}
			entriesRead = er
		default:
			w.WriteError("ERR syntax error")
			return
		}
	}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getStreamSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if exists && wrongType {
			w.WriteWrongType()
			return
		}

		if !exists {
			if !mkStream {
				w.WriteError("ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.")
				return
			}
			// Create empty stream: emit type tag + an INSERT_OP to override any prior DEL REMOVE_OP.
			// MetaEffect alone doesn't change snap.Op from REMOVE_OP.
			if err := emitStreamTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
			if err := cmd.Context.Emit(&pb.Effect{
				Key: []byte(key),
				Kind: &pb.Effect_Data{Data: &pb.DataEffect{
					Op:         pb.EffectOp_INSERT_OP,
					Merge:      pb.MergeRule_LAST_WRITE_WINS,
					Collection: pb.CollectionKind_ORDERED,
				}},
			}); err != nil {
				w.WriteError(err.Error())
				return
			}
			// Reset entries-added to 0 for the new empty stream
			if err := setStreamEntriesAdded(cmd, key, 0); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		// Check if group already exists
		if groupExists(cmd, key, groupName) {
			w.WriteError("BUSYGROUP Consumer Group name already exists")
			return
		}

		// Parse last delivered ID
		var lastDeliveredID shared.StreamID
		switch idStr {
		case "$":
			if exists {
				lastID, _, _, found := streamMetadata(snap)
				if !found {
					entries := snapshotToEntries(snap)
					lastID = streamLastID(entries)
				}
				lastDeliveredID = lastID
			}
		case "0", "0-0":
			lastDeliveredID = shared.StreamID{}
		default:
			id, _, isSpecial, err := shared.ParseStreamID(idStr)
			if err != nil {
				w.WriteError("ERR Invalid stream ID specified as stream command argument")
				return
			}
			if isSpecial {
				w.WriteError("ERR Invalid stream ID specified as stream command argument")
				return
			}
			lastDeliveredID = id
		}

		// Cap entries-read
		if entriesRead >= 0 && exists {
			_, ea, _, _ := streamMetadata(snap)
			if cfgEA := getStreamEntriesAdded(cmd, key); cfgEA >= 0 {
				ea = cfgEA
			}
			if entriesRead > ea {
				entriesRead = ea
			}
		}

		now := time.Now().UnixMilli()
		metaRaw := encodeGroupMeta(lastDeliveredID, entriesRead, now)
		if err := emitGroupInsert(cmd, key, groupName, metaRaw); err != nil {
			w.WriteError(err.Error())
			return
		}

		// Register the group name in the registry
		if err := emitGroupRegistryInsert(cmd, key, groupName); err != nil {
			w.WriteError(err.Error())
			return
		}

		w.WriteSimpleString("OK")
	}
	return
}

// handleXGroupDestroy handles XGROUP DESTROY key group
func handleXGroupDestroy(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("xgroup|destroy")
		return
	}

	key := string(cmd.Args[1])
	groupName := string(cmd.Args[2])
	keys = []string{key}

	valid = true
	runner = func() {
		_, _, exists, wrongType, err := getStreamSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteInteger(0)
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		if !groupExists(cmd, key, groupName) {
			w.WriteInteger(0)
			return
		}

		if err := emitGroupRemove(cmd, key, groupName); err != nil {
			w.WriteError(err.Error())
			return
		}

		// Remove PEL
		if err := emitPELKeyRemove(cmd, key, groupName); err != nil {
			w.WriteError(err.Error())
			return
		}

		// Remove from registry
		if err := emitGroupRegistryRemove(cmd, key, groupName); err != nil {
			w.WriteError(err.Error())
			return
		}

		w.WriteInteger(1)
	}
	return
}

// handleXGroupCreateConsumer handles XGROUP CREATECONSUMER key group consumer
func handleXGroupCreateConsumer(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("xgroup|createconsumer")
		return
	}

	key := string(cmd.Args[1])
	groupName := string(cmd.Args[2])
	consumerName := string(cmd.Args[3])
	keys = []string{key}

	valid = true
	runner = func() {
		_, _, exists, wrongType, err := getStreamSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteError("ERR The XGROUP subcommand requires the key to exist.")
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		if !groupExists(cmd, key, groupName) {
			w.WriteError("NOGROUP No such consumer group '" + groupName + "' for key name '" + key + "'")
			return
		}

		// Check if consumer already exists
		gSnap, _, _ := getGroupSnapshot(cmd, key, groupName)
		consumers := getConsumersFromGroup(gSnap)
		if _, ok := consumers[consumerName]; ok {
			w.WriteInteger(0)
			return
		}

		now := time.Now().UnixMilli()
		metaRaw := encodeConsumerMeta(now, 0)
		if err := emitConsumerInsert(cmd, key, groupName, consumerName, metaRaw); err != nil {
			w.WriteError(err.Error())
			return
		}

		w.WriteInteger(1)
	}
	return
}

// handleXGroupDelConsumer handles XGROUP DELCONSUMER key group consumer
func handleXGroupDelConsumer(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("xgroup|delconsumer")
		return
	}

	key := string(cmd.Args[1])
	groupName := string(cmd.Args[2])
	consumerName := string(cmd.Args[3])
	keys = []string{key}

	valid = true
	runner = func() {
		_, _, exists, wrongType, err := getStreamSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteError("ERR The XGROUP subcommand requires the key to exist.")
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		if !groupExists(cmd, key, groupName) {
			w.WriteError("NOGROUP No such consumer group '" + groupName + "' for key name '" + key + "'")
			return
		}

		// Count pending entries for this consumer
		pelSnap, _, _ := getPELSnapshot(cmd, key, groupName)
		pendingCount := consumerPendingCount(pelSnap, consumerName)

		// Remove pending entries for this consumer
		if pelSnap != nil && pelSnap.NetAdds != nil {
			for elemID, elem := range pelSnap.NetAdds {
				consumer, _, _ := decodePELEntry(elem.Data.Decompress())
				if consumer == consumerName && len(elemID) == 16 {
					id := decodeStreamElementID([]byte(elemID))
					emitPELRemove(cmd, key, groupName, id)
				}
			}
		}

		if err := emitConsumerRemove(cmd, key, groupName, consumerName); err != nil {
			w.WriteError(err.Error())
			return
		}

		w.WriteInteger(pendingCount)
	}
	return
}

// handleXGroupSetID handles XGROUP SETID key group <id|$> [ENTRIESREAD entries-read]
func handleXGroupSetID(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("xgroup|setid")
		return
	}

	key := string(cmd.Args[1])
	groupName := string(cmd.Args[2])
	idStr := string(cmd.Args[3])
	keys = []string{key}

	var entriesRead int64 = -1

	for i := 4; i < len(cmd.Args); i++ {
		opt := string(shared.ToUpper(cmd.Args[i]))
		if opt == "ENTRIESREAD" {
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			er, err := strconv.ParseInt(string(cmd.Args[i]), 10, 64)
			if err != nil {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			if er < -1 {
				w.WriteError("ERR value for ENTRIESREAD must be positive or -1")
				return
			}
			entriesRead = er
		} else {
			w.WriteError("ERR syntax error")
			return
		}
	}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getStreamSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteError("ERR The XGROUP subcommand requires the key to exist.")
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		if !groupExists(cmd, key, groupName) {
			w.WriteError("NOGROUP No such consumer group '" + groupName + "' for key name '" + key + "'")
			return
		}

		var lastDeliveredID shared.StreamID
		switch idStr {
		case "$":
			lastID, _, _, found := streamMetadata(snap)
			if !found {
				entries := snapshotToEntries(snap)
				lastID = streamLastID(entries)
			}
			lastDeliveredID = lastID
		case "0", "0-0", "-":
			lastDeliveredID = shared.StreamID{}
		default:
			id, _, isSpecial, err := shared.ParseStreamID(idStr)
			if err != nil {
				w.WriteError("ERR Invalid stream ID specified as stream command argument")
				return
			}
			if isSpecial {
				w.WriteError("ERR Invalid stream ID specified as stream command argument")
				return
			}
			lastDeliveredID = id
		}

		// Get existing group metadata for createdAt
		gSnap, _, _ := getGroupSnapshot(cmd, key, groupName)
		_, existingEntriesRead, existingCreatedAt, _ := getGroupMeta(gSnap)

		finalEntriesRead := existingEntriesRead
		if entriesRead >= 0 {
			_, ea, _, _ := streamMetadata(snap)
			if cfgEA := getStreamEntriesAdded(cmd, key); cfgEA >= 0 {
				ea = cfgEA
			}
			if entriesRead > ea {
				entriesRead = ea
			}
			finalEntriesRead = entriesRead
		}

		metaRaw := encodeGroupMeta(lastDeliveredID, finalEntriesRead, existingCreatedAt)
		if err := emitGroupInsert(cmd, key, groupName, metaRaw); err != nil {
			w.WriteError(err.Error())
			return
		}

		w.WriteSimpleString("OK")
	}
	return
}

// handleXGroupHelp handles XGROUP HELP
func handleXGroupHelp(w *shared.Writer) {
	help := []string{
		"XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
		"CREATE <key> <groupname> <id|$> [option]",
		"    Create a new consumer group. Options are:",
		"    * MKSTREAM",
		"      Create the empty stream if it does not exist.",
		"    * ENTRIESREAD entries_read",
		"      Set the group's current ID.",
		"SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
		"    Set the current group ID.",
		"DESTROY <key> <groupname>",
		"    Remove the specified group.",
		"CREATECONSUMER <key> <groupname> <consumername>",
		"    Create a consumer named <consumername> in the group.",
		"DELCONSUMER <key> <groupname> <consumername>",
		"    Remove the specified consumer.",
		"HELP",
		"    Print this help.",
	}
	w.WriteArray(len(help))
	for _, line := range help {
		w.WriteBulkStringStr(line)
	}
}

// handleXAck handles XACK key group id [id ...]
func handleXAck(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("xack")
		return
	}

	key := string(cmd.Args[0])
	groupName := string(cmd.Args[1])
	keys = []string{key}

	ids := make([]shared.StreamID, 0, len(cmd.Args)-2)
	for i := 2; i < len(cmd.Args); i++ {
		id, _, isSpecial, err := shared.ParseStreamID(string(cmd.Args[i]))
		if err != nil || isSpecial {
			w.WriteError("ERR Invalid stream ID specified as stream command argument")
			return
		}
		ids = append(ids, id)
	}

	valid = true
	runner = func() {
		_, _, exists, wrongType, err := getStreamSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteInteger(0)
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		if !groupExists(cmd, key, groupName) {
			w.WriteError("NOGROUP No such consumer group '" + groupName + "' for key name '" + key + "'")
			return
		}

		pelSnap, _, _ := getPELSnapshot(cmd, key, groupName)

		acked := int64(0)
		for _, id := range ids {
			elemID := string(encodeStreamElementID(id))
			if pelSnap != nil && pelSnap.NetAdds != nil {
				if _, ok := pelSnap.NetAdds[elemID]; ok {
					if err := emitPELRemove(cmd, key, groupName, id); err != nil {
						w.WriteError(err.Error())
						return
					}
					acked++
				}
			}
		}

		w.WriteInteger(acked)
	}
	return
}

// handleXPending handles XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
func handleXPending(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("xpending")
		return
	}

	key := string(cmd.Args[0])
	groupName := string(cmd.Args[1])
	keys = []string{key}

	valid = true
	runner = func() {
		_, _, exists, wrongType, err := getStreamSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteError("ERR no such key")
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		if !groupExists(cmd, key, groupName) {
			w.WriteError("NOGROUP No such consumer group '" + groupName + "' for key name '" + key + "'")
			return
		}

		pelSnap, _, _ := getPELSnapshot(cmd, key, groupName)
		pelEntries := pelToEntries(pelSnap)

		// Summary form: XPENDING key group
		if len(cmd.Args) == 2 {
			writePendingSummary(w, pelEntries)
			return
		}

		// Extended form: XPENDING key group [IDLE min-idle] start end count [consumer]
		argIdx := 2
		var minIdle int64 = -1

		if string(shared.ToUpper(cmd.Args[argIdx])) == "IDLE" {
			argIdx++
			if argIdx >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			mi, err := strconv.ParseInt(string(cmd.Args[argIdx]), 10, 64)
			if err != nil || mi < 0 {
				w.WriteError("ERR Invalid min-idle-time argument for XPENDING")
				return
			}
			minIdle = mi
			argIdx++
		}

		if argIdx+2 >= len(cmd.Args) {
			w.WriteError("ERR syntax error")
			return
		}

		startStr := string(cmd.Args[argIdx])
		endStr := string(cmd.Args[argIdx+1])
		countStr := string(cmd.Args[argIdx+2])
		argIdx += 3

		start, startExcl, err := shared.ParseStreamIDForRange(startStr, false)
		if err != nil {
			w.WriteError("ERR Invalid stream ID specified as stream command argument")
			return
		}
		end, endExcl, err := shared.ParseStreamIDForRange(endStr, true)
		if err != nil {
			w.WriteError("ERR Invalid stream ID specified as stream command argument")
			return
		}
		count, err := strconv.Atoi(countStr)
		if err != nil || count < 0 {
			w.WriteError("ERR value is not an integer or out of range")
			return
		}

		var consumerFilter string
		if argIdx < len(cmd.Args) {
			consumerFilter = string(cmd.Args[argIdx])
		}

		now := time.Now().UnixMilli()

		// Filter entries
		var filtered []*shared.PendingEntry
		for _, pe := range pelEntries {
			// Consumer filter
			if consumerFilter != "" && pe.Consumer != consumerFilter {
				continue
			}
			// Range filter
			cmpStart := pe.ID.Compare(start)
			if startExcl && cmpStart <= 0 {
				continue
			}
			if !startExcl && cmpStart < 0 {
				continue
			}
			cmpEnd := pe.ID.Compare(end)
			if endExcl && cmpEnd >= 0 {
				continue
			}
			if !endExcl && cmpEnd > 0 {
				continue
			}
			// Idle filter
			if minIdle >= 0 {
				idle := now - pe.DeliveryTime
				if idle < minIdle {
					continue
				}
			}
			filtered = append(filtered, pe)
			if count > 0 && len(filtered) >= count {
				break
			}
		}

		w.WriteArray(len(filtered))
		for _, pe := range filtered {
			w.WriteArray(4)
			w.WriteBulkStringStr(pe.ID.String())
			w.WriteBulkStringStr(pe.Consumer)
			idle := now - pe.DeliveryTime
			w.WriteInteger(idle)
			w.WriteInteger(pe.DeliveryCount)
		}
	}
	return
}

func writePendingSummary(w *shared.Writer, pelEntries []*shared.PendingEntry) {
	if len(pelEntries) == 0 {
		w.WriteArray(4)
		w.WriteInteger(0)
		w.WriteNullBulkString()
		w.WriteNullBulkString()
		w.WriteNullArray()
		return
	}

	// Count per consumer
	consumerCounts := make(map[string]int64)
	for _, pe := range pelEntries {
		consumerCounts[pe.Consumer]++
	}

	w.WriteArray(4)
	w.WriteInteger(int64(len(pelEntries)))
	w.WriteBulkStringStr(pelEntries[0].ID.String())
	w.WriteBulkStringStr(pelEntries[len(pelEntries)-1].ID.String())

	// Consumer list sorted
	names := make([]string, 0, len(consumerCounts))
	for name := range consumerCounts {
		names = append(names, name)
	}
	sort.Strings(names)

	w.WriteArray(len(names))
	for _, name := range names {
		w.WriteArray(2)
		w.WriteBulkStringStr(name)
		w.WriteBulkStringStr(strconv.FormatInt(consumerCounts[name], 10))
	}
}

// handleXReadGroup handles XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] [CLAIM min-idle-time] [NOACK] STREAMS key [key ...] id [id ...]
func handleXReadGroup(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
	if conn.InScript {
		handleXReadGroupNonBlocking(cmd, w, db)
		return
	}

	if len(cmd.Args) < 6 {
		w.WriteWrongNumArguments("xreadgroup")
		return
	}

	if string(shared.ToUpper(cmd.Args[0])) != "GROUP" {
		w.WriteError("ERR syntax error")
		return
	}

	groupName := string(cmd.Args[1])
	consumerName := string(cmd.Args[2])

	var count int
	var block float64 = -1
	var noAck bool
	var claimMinIdle int64 = -1
	i := 3

	for i < len(cmd.Args) {
		opt := shared.ToUpper(cmd.Args[i])
		switch string(opt) {
		case "COUNT":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			c, err := strconv.Atoi(string(cmd.Args[i]))
			if err != nil || c < 0 {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			count = c
			i++
		case "BLOCK":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			b, err := strconv.ParseInt(string(cmd.Args[i]), 10, 64)
			if err != nil {
				w.WriteError("ERR timeout is not an integer or out of range")
				return
			}
			if b < 0 {
				w.WriteError("ERR timeout is negative")
				return
			}
			block = float64(b) / 1000.0
			i++
		case "CLAIM":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			minIdleStr := string(cmd.Args[i])
			if len(minIdleStr) > 0 && minIdleStr[0] == '+' {
				w.WriteError("ERR min-idle-time is not an integer or out of range")
				return
			}
			if minIdleStr == "-0" {
				w.WriteError("ERR min-idle-time is not an integer or out of range")
				return
			}
			minIdle, err := strconv.ParseInt(minIdleStr, 10, 64)
			if err != nil {
				w.WriteError("ERR min-idle-time is not an integer or out of range")
				return
			}
			if minIdle < 0 {
				w.WriteError("ERR min-idle-time must be a positive integer")
				return
			}
			claimMinIdle = minIdle
			i++
		case "NOACK":
			noAck = true
			i++
		case "STREAMS":
			i++
			goto parseRGStreams
		default:
			w.WriteError("ERR syntax error")
			return
		}
	}

	w.WriteError("ERR syntax error")
	return

parseRGStreams:
	remaining := cmd.Args[i:]
	if len(remaining) < 2 || len(remaining)%2 != 0 {
		w.WriteError("ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.")
		return
	}

	numStreams := len(remaining) / 2
	streamKeys := make([]string, numStreams)
	useNewOnly := make([]bool, numStreams)
	ids := make([]shared.StreamID, numStreams)

	for j := range numStreams {
		streamKeys[j] = string(remaining[j])
		idStr := string(remaining[numStreams+j])

		if idStr == ">" {
			useNewOnly[j] = true
		} else {
			id, _, isSpecial, err := shared.ParseStreamID(idStr)
			if err != nil {
				w.WriteError("ERR Invalid stream ID specified as stream command argument")
				return
			}
			if isSpecial && idStr != "0" && idStr != "0-0" {
				w.WriteError("ERR Invalid stream ID specified as stream command argument")
				return
			}
			ids[j] = id
		}
	}

	hasNewOnly := false
	for _, u := range useNewOnly {
		if u {
			hasNewOnly = true
			break
		}
	}

	if claimMinIdle >= 0 && hasNewOnly {
		tryClaimStreams := func() ([]streamResultWithClaim, bool) {
			results, noGrp, wrongT := doXReadGroupClaim(cmd, streamKeys, useNewOnly, groupName, consumerName, count, noAck, claimMinIdle)
			if noGrp {
				w.WriteError("NOGROUP No such key '" + streamKeys[0] + "' or consumer group '" + groupName + "' in XREADGROUP with GROUP option")
				return nil, false
			}
			if wrongT {
				w.WriteWrongType()
				return nil, false
			}
			return results, true
		}

		// Non-blocking CLAIM
		if block < 0 {
			results, ok := tryClaimStreams()
			if !ok {
				return
			}
			if len(results) == 0 {
				w.WriteNullArray()
				return
			}
			writeXReadResultsWithClaim(w, results)
			return
		}

		// Blocking CLAIM — reuses the same blocking infrastructure as regular XREADGROUP
		ctx, cleanup := conn.Block(block)
		defer cleanup()

		keyBytes := make([][]byte, 0, len(streamKeys)*2)
		for _, k := range streamKeys {
			keyBytes = append(keyBytes, []byte(k))
			keyBytes = append(keyBytes, []byte(groupKey(k, groupName)))
		}
		reg, _ := db.Manager().Subscriptions.RegisterWithNoKey(ctx, keyBytes...)
		defer reg.Cancel()

		for {
			results, ok := tryClaimStreams()
			if !ok {
				return
			}
			if len(results) > 0 {
				reg.Cancel()
				writeXReadResultsWithClaim(w, results)
				return
			}

			// Check if any PEL entries will become claimable soon — use a timer
			now := time.Now().UnixMilli()
			earliest := int64(-1)
			for _, sKey := range streamKeys {
				pelSnap, _, _ := getPELSnapshot(cmd, sKey, groupName)
				for _, pe := range pelToEntries(pelSnap) {
					claimableAt := pe.DeliveryTime + claimMinIdle
					if earliest < 0 || claimableAt < earliest {
						earliest = claimableAt
					}
				}
			}

			if earliest > 0 && earliest > now {
				timer := time.NewTimer(time.Duration(earliest-now) * time.Millisecond)
				select {
				case <-ctx.Done():
					timer.Stop()
					if conn.WasUnblockedWithError() {
						w.WriteError("UNBLOCKED client unblocked via CLIENT UNBLOCK")
						return
					}
					w.WriteNullArray()
					return
				case <-timer.C:
					continue
				case <-reg.NotifyChan():
					timer.Stop()
					continue
				}
			}

			if _, err := reg.Wait(); err != nil {
				if conn.WasUnblockedWithError() {
					w.WriteError("UNBLOCKED client unblocked via CLIENT UNBLOCK")
					return
				}
				w.WriteNullArray()
				return
			}
		}
	}

	tryReadGroupStreams := func() ([]streamResult, bool) {
		results, noGrp, wrongT := doXReadGroup(cmd, streamKeys, useNewOnly, ids, groupName, consumerName, count, noAck)
		if noGrp {
			w.WriteError("NOGROUP No such consumer group '" + groupName + "' for key name '" + streamKeys[0] + "'")
			return nil, false
		}
		if wrongT {
			w.WriteWrongType()
			return nil, false
		}
		return results, true
	}

	// Non-blocking
	if block < 0 {
		results, ok := tryReadGroupStreams()
		if !ok {
			return
		}
		if len(results) == 0 {
			w.WriteNullArray()
			return
		}
		writeXReadResults(w, results)
		return
	}

	if !hasNewOnly {
		results, ok := tryReadGroupStreams()
		if !ok {
			return
		}
		writeXReadGroupHistoryResults(w, results, streamKeys)
		return
	}

	// Blocking
	ctx, cleanup := conn.Block(block)
	defer cleanup()

	// Register on both the stream keys (for new data) and the group subkeys
	// (for XGROUP DESTROY notifications).
	keyBytes := make([][]byte, 0, len(streamKeys)*2)
	for _, k := range streamKeys {
		keyBytes = append(keyBytes, []byte(k))
		keyBytes = append(keyBytes, []byte(groupKey(k, groupName)))
	}
	reg, _ := db.Manager().Subscriptions.RegisterWithNoKey(ctx, keyBytes...)
	defer reg.Cancel()

	for {
		results, ok := tryReadGroupStreams()
		if !ok {
			// Error already written (NOGROUP, WRONGTYPE)
			return
		}
		if len(results) > 0 {
			reg.Cancel()
			writeXReadResults(w, results)
			return
		}

		if _, err := reg.Wait(); err != nil {
			if conn.WasUnblockedWithError() {
				w.WriteError("UNBLOCKED client unblocked via CLIENT UNBLOCK")
				return
			}
			w.WriteNullArray()
			return
		}
	}
}

// handleXReadGroupNonBlocking is the transaction-safe variant
func handleXReadGroupNonBlocking(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 6 {
		w.WriteWrongNumArguments("xreadgroup")
		return
	}

	if string(shared.ToUpper(cmd.Args[0])) != "GROUP" {
		w.WriteError("ERR syntax error")
		return
	}

	groupName := string(cmd.Args[1])
	consumerName := string(cmd.Args[2])

	var count int
	var noAck bool
	i := 3

	for i < len(cmd.Args) {
		opt := shared.ToUpper(cmd.Args[i])
		switch string(opt) {
		case "COUNT":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			c, err := strconv.Atoi(string(cmd.Args[i]))
			if err != nil || c < 0 {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			count = c
			i++
		case "BLOCK":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			i++ // skip value
		case "CLAIM":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			i++ // skip value
		case "NOACK":
			noAck = true
			i++
		case "STREAMS":
			i++
			goto parseNBRGStreams
		default:
			w.WriteError("ERR syntax error")
			return
		}
	}

	w.WriteError("ERR syntax error")
	return

parseNBRGStreams:
	remaining := cmd.Args[i:]
	if len(remaining) < 2 || len(remaining)%2 != 0 {
		w.WriteError("ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.")
		return
	}

	numStreams := len(remaining) / 2
	streamKeys := make([]string, numStreams)
	useNewOnly := make([]bool, numStreams)
	ids := make([]shared.StreamID, numStreams)

	for j := range numStreams {
		streamKeys[j] = string(remaining[j])
		idStr := string(remaining[numStreams+j])

		if idStr == ">" {
			useNewOnly[j] = true
		} else {
			id, _, isSpecial, err := shared.ParseStreamID(idStr)
			if err != nil {
				w.WriteError("ERR Invalid stream ID specified as stream command argument")
				return
			}
			if isSpecial && idStr != "0" && idStr != "0-0" {
				w.WriteError("ERR Invalid stream ID specified as stream command argument")
				return
			}
			ids[j] = id
		}
	}

	keys = streamKeys
	valid = true
	runner = func() {
		results, noGrp, wrongT := doXReadGroup(cmd, streamKeys, useNewOnly, ids, groupName, consumerName, count, noAck)
		if noGrp {
			w.WriteError("NOGROUP No such consumer group '" + groupName + "' for key name '" + streamKeys[0] + "'")
			return
		}
		if wrongT {
			w.WriteWrongType()
			return
		}
		if len(results) == 0 {
			w.WriteNullArray()
			return
		}
		writeXReadResults(w, results)
	}
	return
}

// doXReadGroup performs the actual XREADGROUP logic.
func doXReadGroup(cmd *shared.Command, streamKeys []string, useNewOnly []bool, ids []shared.StreamID, groupName, consumerName string, count int, noAck bool) (results []streamResult, noGroup bool, wrongTypeKey bool) {

	for j, sKey := range streamKeys {
		snap, _, exists, wrongType, err := getStreamSnapshot(cmd, sKey)
		if err != nil {
			continue
		}
		if wrongType {
			wrongTypeKey = true
			return
		}

		if !exists || !groupExists(cmd, sKey, groupName) {
			noGroup = true
			return
		}

		if useNewOnly[j] {
			// New entries mode (>)
			gSnap, _, _ := getGroupSnapshot(cmd, sKey, groupName)
			lastDelivered, entriesRead, createdAt, _ := getGroupMeta(gSnap)

			entries := snapshotToEntries(snap)
			filtered := rangeEntries(entries, lastDelivered, shared.MaxStreamID, true, false, count)

			if len(filtered) > 0 {
				now := time.Now().UnixMilli()

				// Update last delivered ID
				newLastDelivered := filtered[len(filtered)-1].ID
				newEntriesRead := entriesRead
				if newEntriesRead >= 0 {
					newEntriesRead += int64(len(filtered))
				} else {
					// First delivery: initialize entries-read from the count of entries delivered
					newEntriesRead = int64(len(filtered))
				}
				metaRaw := encodeGroupMeta(newLastDelivered, newEntriesRead, createdAt)
				emitGroupInsert(cmd, sKey, groupName, metaRaw)

				// Ensure consumer exists
				consumerMeta := encodeConsumerMeta(now, now)
				emitConsumerInsert(cmd, sKey, groupName, consumerName, consumerMeta)

				// Add to PEL unless NOACK
				if !noAck {
					for _, entry := range filtered {
						pelRaw := encodePELEntry(consumerName, now, 1)
						emitPELInsert(cmd, sKey, groupName, entry.ID, pelRaw)
					}
				}

				results = append(results, streamResult{key: sKey, entries: filtered})
			}
		} else {
			// History mode (specific ID) — read from PEL
			pelSnap, _, _ := getPELSnapshot(cmd, sKey, groupName)
			if pelSnap == nil || pelSnap.NetAdds == nil {
				continue
			}

			startID := ids[j]
			entries := snapshotToEntries(snap)

			var pendingEntries []*shared.StreamEntry
			pelEntries := pelToEntries(pelSnap)
			for _, pe := range pelEntries {
				if pe.Consumer != consumerName {
					continue
				}
				if pe.ID.Compare(startID) <= 0 {
					continue
				}
				// Find the entry in the stream; if deleted, return ID with empty fields
				found := false
				for _, entry := range entries {
					if entry.ID.Compare(pe.ID) == 0 {
						pendingEntries = append(pendingEntries, entry)
						found = true
						break
					}
				}
				if !found {
					// Entry was deleted — return placeholder with empty fields
					pendingEntries = append(pendingEntries, &shared.StreamEntry{ID: pe.ID, Fields: nil})
				}
				if count > 0 && len(pendingEntries) >= count {
					break
				}
			}

			// Update consumer seen time
			now := time.Now().UnixMilli()
			consumerMeta := encodeConsumerMeta(now, 0)
			gSnap, _, _ := getGroupSnapshot(cmd, sKey, groupName)
			consumers := getConsumersFromGroup(gSnap)
			if elem, ok := consumers[consumerName]; ok {
				_, activeTime := decodeConsumerMeta(elem.Data.Decompress())
				consumerMeta = encodeConsumerMeta(now, activeTime)
			}
			emitConsumerInsert(cmd, sKey, groupName, consumerName, consumerMeta)

			if len(pendingEntries) > 0 {
				results = append(results, streamResult{key: sKey, entries: pendingEntries})
			}
		}
	}

	return results, false, false
}

// doXReadGroupClaim performs XREADGROUP with the CLAIM option.
// It claims pending entries idle >= claimMinIdle, then reads new entries from ">".
// Returns results in claimEntry format with idle_time and delivery_count metadata.
func doXReadGroupClaim(cmd *shared.Command, streamKeys []string, useNewOnly []bool, groupName, consumerName string, count int, noAck bool, claimMinIdle int64) (results []streamResultWithClaim, noGroup bool, wrongTypeKey bool) {
	for j, sKey := range streamKeys {
		snap, _, exists, wrongType, err := getStreamSnapshot(cmd, sKey)
		if err != nil {
			continue
		}
		if wrongType {
			wrongTypeKey = true
			return
		}

		if !exists || !groupExists(cmd, sKey, groupName) {
			noGroup = true
			return
		}

		now := time.Now().UnixMilli()
		entries := snapshotToEntries(snap)
		var claimEntries []claimEntry
		remaining := count
		if remaining <= 0 {
			remaining = 100
		}

		// Step 1: Claim pending entries idle >= claimMinIdle
		if useNewOnly[j] {
			pelSnap, _, _ := getPELSnapshot(cmd, sKey, groupName)
			pelEntries := pelToEntries(pelSnap)

			for _, pe := range pelEntries {
				if remaining <= 0 {
					break
				}
				idle := now - pe.DeliveryTime
				if idle < claimMinIdle {
					continue
				}
				// Find entry in stream
				var entry *shared.StreamEntry
				for _, e := range entries {
					if e.ID.Compare(pe.ID) == 0 {
						entry = e
						break
					}
				}
				if entry == nil {
					continue // deleted entry, skip
				}

				// Claim: transfer ownership, increment delivery count in PEL
				pelRaw := encodePELEntry(consumerName, now, pe.DeliveryCount+1)
				emitPELInsert(cmd, sKey, groupName, pe.ID, pelRaw)

				claimEntries = append(claimEntries, claimEntry{
					entry:         entry,
					idleTime:      idle,
					deliveryCount: pe.DeliveryCount, // pre-increment value in response
				})
				remaining--
			}

			// Step 2: Read new entries from ">"
			if remaining > 0 {
				gSnap, _, _ := getGroupSnapshot(cmd, sKey, groupName)
				lastDelivered, entriesRead, createdAt, _ := getGroupMeta(gSnap)

				filtered := rangeEntries(entries, lastDelivered, shared.MaxStreamID, true, false, remaining)

				if len(filtered) > 0 {
					// Update last delivered ID
					newLastDelivered := filtered[len(filtered)-1].ID
					newEntriesRead := entriesRead
					if newEntriesRead >= 0 {
						newEntriesRead += int64(len(filtered))
					} else {
						newEntriesRead = int64(len(filtered))
					}
					metaRaw := encodeGroupMeta(newLastDelivered, newEntriesRead, createdAt)
					emitGroupInsert(cmd, sKey, groupName, metaRaw)

					// Add to PEL unless NOACK
					if !noAck {
						for _, entry := range filtered {
							pelRaw := encodePELEntry(consumerName, now, 1)
							emitPELInsert(cmd, sKey, groupName, entry.ID, pelRaw)
						}
					}

					for _, entry := range filtered {
						claimEntries = append(claimEntries, claimEntry{
							entry:         entry,
							idleTime:      0,
							deliveryCount: 0,
						})
					}
				}
			}
		}

		// Ensure consumer exists
		if len(claimEntries) > 0 {
			consumerMeta := encodeConsumerMeta(now, now)
			emitConsumerInsert(cmd, sKey, groupName, consumerName, consumerMeta)
		}

		if len(claimEntries) > 0 {
			results = append(results, streamResultWithClaim{key: sKey, entries: claimEntries})
		}
	}

	return results, false, false
}

// handleXAckDel handles XACKDEL key group [KEEPREF|DELREF|ACKED] IDS numids id [id ...]
func handleXAckDel(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 5 {
		w.WriteWrongNumArguments("xackdel")
		return
	}

	key := string(cmd.Args[0])
	groupName := string(cmd.Args[1])
	keys = []string{key}
	i := 2

	refMode := shared.RefModeKeepRef
	if i < len(cmd.Args) {
		opt := string(shared.ToUpper(cmd.Args[i]))
		switch opt {
		case "KEEPREF":
			refMode = shared.RefModeKeepRef
			i++
		case "DELREF":
			refMode = shared.RefModeDelRef
			i++
		case "ACKED":
			refMode = shared.RefModeAcked
			i++
		case "IDS":
			// fall through
		default:
			w.WriteError("ERR syntax error")
			return
		}
	}

	if i >= len(cmd.Args) || string(shared.ToUpper(cmd.Args[i])) != "IDS" {
		w.WriteError("ERR syntax error")
		return
	}
	i++

	if i >= len(cmd.Args) {
		w.WriteError("ERR syntax error")
		return
	}

	numIds, err := strconv.Atoi(string(cmd.Args[i]))
	if err != nil || numIds < 1 {
		w.WriteError("ERR Number of IDs must be a positive integer")
		return
	}
	i++

	if i+numIds != len(cmd.Args) {
		if i+numIds > len(cmd.Args) {
			w.WriteError("ERR The `numids` parameter must match the number of arguments")
		} else {
			w.WriteError("ERR syntax error")
		}
		return
	}

	ids := make([]shared.StreamID, numIds)
	for j := range numIds {
		id, _, isSpecial, err := shared.ParseStreamID(string(cmd.Args[i+j]))
		if err != nil || isSpecial {
			w.WriteError("ERR Invalid stream ID specified as stream command argument")
			return
		}
		ids[j] = id
	}

	valid = true
	runner = func() {
		_, _, exists, wrongType, err := getStreamSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteArray(numIds)
			for range numIds {
				w.WriteInteger(-1)
			}
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		if !groupExists(cmd, key, groupName) {
			w.WriteArray(numIds)
			for range numIds {
				w.WriteInteger(-1)
			}
			return
		}

		pelSnap, _, _ := getPELSnapshot(cmd, key, groupName)

		results := make([]int64, numIds)
		for j, id := range ids {
			elemID := string(encodeStreamElementID(id))
			if pelSnap == nil || pelSnap.NetAdds == nil {
				results[j] = -1
				continue
			}
			if _, ok := pelSnap.NetAdds[elemID]; !ok {
				results[j] = -1
				continue
			}

			// Remove from this group's PEL
			emitPELRemove(cmd, key, groupName, id)

			switch refMode {
			case shared.RefModeKeepRef:
				// Ack + delete stream entry, but don't touch other groups' PELs
				emitStreamRemove(cmd, key, id)
				results[j] = 1

			case shared.RefModeDelRef:
				// Ack + delete from ALL groups' PELs + delete stream entry
				for _, g := range listGroupNames(cmd, key) {
					if g == groupName {
						continue
					}
					emitPELRemove(cmd, key, g, id)
				}
				emitStreamRemove(cmd, key, id)
				results[j] = 1

			case shared.RefModeAcked:
				// Ack; only delete stream entry if no other group still has it pending
				allAcked := true
				for _, g := range listGroupNames(cmd, key) {
					if g == groupName {
						continue
					}
					otherPEL, _, _ := getPELSnapshot(cmd, key, g)
					if otherPEL != nil && otherPEL.NetAdds != nil {
						if _, ok := otherPEL.NetAdds[elemID]; ok {
							allAcked = false
							break
						}
					}
				}
				if allAcked {
					emitStreamRemove(cmd, key, id)
					results[j] = 1
				} else {
					results[j] = 2
				}
			}
		}

		w.WriteArray(numIds)
		for _, r := range results {
			w.WriteInteger(r)
		}
	}
	return
}

// handleXClaim handles XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms-unix-time] [RETRYCOUNT count] [FORCE] [JUSTID] [LASTID id]
func handleXClaim(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 5 {
		w.WriteWrongNumArguments("xclaim")
		return
	}

	key := string(cmd.Args[0])
	groupName := string(cmd.Args[1])
	consumerName := string(cmd.Args[2])
	keys = []string{key}

	minIdleStr := string(cmd.Args[3])
	minIdleTime, err := strconv.ParseInt(minIdleStr, 10, 64)
	if err != nil || minIdleTime < 0 {
		w.WriteError("ERR Invalid min-idle-time argument for XCLAIM")
		return
	}

	// Parse IDs and options
	var claimIDs []shared.StreamID
	var force, justID bool
	var idleMs int64 = -1
	var timeMs int64 = -1
	var retryCount int64 = -1

	for i := 4; i < len(cmd.Args); i++ {
		arg := string(shared.ToUpper(cmd.Args[i]))
		switch arg {
		case "IDLE":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			v, err := strconv.ParseInt(string(cmd.Args[i]), 10, 64)
			if err != nil {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			idleMs = v
		case "TIME":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			v, err := strconv.ParseInt(string(cmd.Args[i]), 10, 64)
			if err != nil {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			timeMs = v
		case "RETRYCOUNT":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			v, err := strconv.ParseInt(string(cmd.Args[i]), 10, 64)
			if err != nil {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			retryCount = v
		case "FORCE":
			force = true
		case "JUSTID":
			justID = true
		case "LASTID":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			// Parse but ignore LASTID for now
		default:
			// Try as stream ID
			id, _, isSpecial, err := shared.ParseStreamID(string(cmd.Args[i]))
			if err != nil || isSpecial {
				w.WriteError("ERR Invalid stream ID specified as stream command argument")
				return
			}
			claimIDs = append(claimIDs, id)
		}
	}

	valid = true
	runner = func() {
		cmd.Context.BeginTx()

		snap, _, exists, wrongType, err := getStreamSnapshot(cmd, key)
		if err != nil {
			cmd.Context.Abort()
			w.WriteError(err.Error())
			return
		}
		if !exists {
			cmd.Context.Abort()
			w.WriteArray(0)
			return
		}
		if wrongType {
			cmd.Context.Abort()
			w.WriteWrongType()
			return
		}

		if !groupExists(cmd, key, groupName) {
			cmd.Context.Abort()
			w.WriteError("NOGROUP No such consumer group '" + groupName + "' for key name '" + key + "'")
			return
		}

		pelSnap, _, _ := getPELSnapshot(cmd, key, groupName)
		entries := snapshotToEntries(snap)
		now := time.Now().UnixMilli()

		var claimed []*shared.StreamEntry
		for _, id := range claimIDs {
			elemID := string(encodeStreamElementID(id))

			// Check if entry exists in PEL
			var pe *shared.PendingEntry
			if pelSnap != nil && pelSnap.NetAdds != nil {
				if elem, ok := pelSnap.NetAdds[elemID]; ok {
					consumer, deliveryTime, deliveryCount := decodePELEntry(elem.Data.Decompress())
					pe = &shared.PendingEntry{
						ID:            id,
						Consumer:      consumer,
						DeliveryTime:  deliveryTime,
						DeliveryCount: deliveryCount,
					}
				}
			}

			if pe == nil && !force {
				continue
			}

			// Check idle time
			if pe != nil && minIdleTime > 0 {
				idle := now - pe.DeliveryTime
				if idle < minIdleTime {
					continue
				}
			}

			// Find entry in stream
			var entry *shared.StreamEntry
			for _, e := range entries {
				if e.ID.Compare(id) == 0 {
					entry = e
					break
				}
			}

			// Claim: update PEL — transfer ownership regardless of stream entry existence.
			// Even if the entry was deleted (XDEL), the PEL ownership must transfer.
			deliveryTime := now
			if idleMs >= 0 {
				deliveryTime = now - idleMs
			}
			if timeMs >= 0 {
				deliveryTime = timeMs
			}
			var delCount int64 = 1
			if pe != nil {
				if justID {
					delCount = pe.DeliveryCount // JUSTID: don't increment
				} else {
					delCount = pe.DeliveryCount + 1
				}
			}
			if retryCount >= 0 {
				delCount = retryCount
			}

			pelRaw := encodePELEntry(consumerName, deliveryTime, delCount)
			emitPELInsert(cmd, key, groupName, id, pelRaw)

			// Only include existing entries in the response
			if entry != nil {
				claimed = append(claimed, entry)
			}
		}

		// Ensure consumer exists
		consumerMeta := encodeConsumerMeta(now, now)
		emitConsumerInsert(cmd, key, groupName, consumerName, consumerMeta)

		if justID {
			w.WriteArray(len(claimed))
			for _, e := range claimed {
				w.WriteBulkStringStr(e.ID.String())
			}
		} else {
			writeStreamEntries(w, claimed)
		}
	}
	return
}

// handleXAutoClaim handles XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
func handleXAutoClaim(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 5 {
		w.WriteWrongNumArguments("xautoclaim")
		return
	}

	key := string(cmd.Args[0])
	groupName := string(cmd.Args[1])
	consumerName := string(cmd.Args[2])
	keys = []string{key}

	minIdleTime, err := strconv.ParseInt(string(cmd.Args[3]), 10, 64)
	if err != nil || minIdleTime < 0 {
		w.WriteError("ERR Invalid min-idle-time argument for XAUTOCLAIM")
		return
	}

	startID, _, _, err := shared.ParseStreamID(string(cmd.Args[4]))
	if err != nil {
		w.WriteError("ERR Invalid stream ID specified as stream command argument")
		return
	}

	count := 100
	justID := false

	for i := 5; i < len(cmd.Args); i++ {
		opt := string(shared.ToUpper(cmd.Args[i]))
		switch opt {
		case "COUNT":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			c, err := strconv.ParseInt(string(cmd.Args[i]), 10, 32)
			if err != nil || c < 1 {
				w.WriteError("ERR COUNT must be > 0")
				return
			}
			count = int(c)
		case "JUSTID":
			justID = true
		default:
			w.WriteError("ERR syntax error")
			return
		}
	}

	valid = true
	runner = func() {
		cmd.Context.BeginTx()

		snap, _, exists, wrongType, err := getStreamSnapshot(cmd, key)
		if err != nil {
			cmd.Context.Abort()
			w.WriteError(err.Error())
			return
		}
		if !exists {
			cmd.Context.Abort()
			w.WriteArray(3)
			w.WriteBulkStringStr("0-0")
			w.WriteArray(0)
			w.WriteArray(0)
			return
		}
		if wrongType {
			cmd.Context.Abort()
			w.WriteWrongType()
			return
		}

		if !groupExists(cmd, key, groupName) {
			cmd.Context.Abort()
			w.WriteError("NOGROUP No such consumer group '" + groupName + "' for key name '" + key + "'")
			return
		}

		pelSnap, _, _ := getPELSnapshot(cmd, key, groupName)
		pelEntries := pelToEntries(pelSnap)
		entries := snapshotToEntries(snap)
		now := time.Now().UnixMilli()

		var claimed []*shared.StreamEntry
		var deletedIDs []shared.StreamID
		cursor := shared.StreamID{Ms: 0, Seq: 0}
		claimed_count := 0

		for _, pe := range pelEntries {
			if pe.ID.Compare(startID) < 0 {
				continue
			}
			if claimed_count >= count {
				cursor = pe.ID
				break
			}

			idle := now - pe.DeliveryTime
			if idle < minIdleTime {
				claimed_count++
				continue
			}

			// Find entry
			var entry *shared.StreamEntry
			for _, e := range entries {
				if e.ID.Compare(pe.ID) == 0 {
					entry = e
					break
				}
			}

			if entry == nil {
				deletedIDs = append(deletedIDs, pe.ID)
				emitPELRemove(cmd, key, groupName, pe.ID)
				claimed_count++
				continue
			}

			// Claim
			pelRaw := encodePELEntry(consumerName, now, pe.DeliveryCount+1)
			emitPELInsert(cmd, key, groupName, pe.ID, pelRaw)
			claimed = append(claimed, entry)
			claimed_count++
		}

		// Ensure consumer exists
		consumerMeta := encodeConsumerMeta(now, now)
		emitConsumerInsert(cmd, key, groupName, consumerName, consumerMeta)

		// Write response: [cursor, entries, deleted_ids]
		w.WriteArray(3)
		w.WriteBulkStringStr(cursor.String())

		if justID {
			w.WriteArray(len(claimed))
			for _, e := range claimed {
				w.WriteBulkStringStr(e.ID.String())
			}
		} else {
			writeStreamEntries(w, claimed)
		}

		w.WriteArray(len(deletedIDs))
		for _, id := range deletedIDs {
			w.WriteBulkStringStr(id.String())
		}
	}
	return
}
