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

package hash

import (
	"fmt"
	"strings"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/redis/shared"
)

// parseHExpireArgs parses the common arguments for HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT
// Returns: (nx, xx, gt, lt, fields, error)
func parseHExpireArgs(args [][]byte, startIdx int) (bool, bool, bool, bool, []string, error) {
	var nx, xx, gt, lt bool
	idx := startIdx

	// Parse optional NX/XX/GT/LT (all mutually exclusive)
	for idx < len(args) {
		arg := strings.ToUpper(string(args[idx]))
		switch arg {
		case "NX":
			if xx || gt || lt {
				return false, false, false, false, nil, fmt.Errorf("ERR Multiple condition flags specified")
			}
			nx = true
			idx++
		case "XX":
			if nx || gt || lt {
				return false, false, false, false, nil, fmt.Errorf("ERR Multiple condition flags specified")
			}
			xx = true
			idx++
		case "GT":
			if nx || xx || lt {
				return false, false, false, false, nil, fmt.Errorf("ERR Multiple condition flags specified")
			}
			gt = true
			idx++
		case "LT":
			if nx || xx || gt {
				return false, false, false, false, nil, fmt.Errorf("ERR Multiple condition flags specified")
			}
			lt = true
			idx++
		case "FIELDS":
			goto parseFields
		default:
			return false, false, false, false, nil, fmt.Errorf("ERR unknown argument '%s'", string(args[idx]))
		}
	}

parseFields:

	// Must have FIELDS keyword
	if idx >= len(args) || strings.ToUpper(string(args[idx])) != "FIELDS" {
		return false, false, false, false, nil, fmt.Errorf("ERR syntax error")
	}
	idx++

	// Must have numfields
	if idx >= len(args) {
		return false, false, false, false, nil, fmt.Errorf("ERR syntax error")
	}
	numFields, ok := shared.ParseInt64(args[idx])
	if !ok {
		return false, false, false, false, nil, fmt.Errorf("ERR value is not an integer or out of range")
	}
	if numFields <= 0 {
		return false, false, false, false, nil, fmt.Errorf("ERR Parameter `numFields` should be greater than 0")
	}
	idx++

	// Must have at least numFields field names
	if int64(len(args)-idx) < numFields {
		return false, false, false, false, nil, fmt.Errorf("wrong number of arguments")
	}

	fields := make([]string, numFields)
	for i := range numFields {
		fields[i] = string(args[idx+int(i)])
	}
	idx += int(numFields)

	// Parse any trailing condition flags (NX/XX/GT/LT can appear after fields too)
	for idx < len(args) {
		arg := strings.ToUpper(string(args[idx]))
		switch arg {
		case "NX":
			if nx || xx || gt || lt {
				return false, false, false, false, nil, fmt.Errorf("ERR Multiple condition flags specified")
			}
			nx = true
			idx++
		case "XX":
			if nx || xx || gt || lt {
				return false, false, false, false, nil, fmt.Errorf("ERR Multiple condition flags specified")
			}
			xx = true
			idx++
		case "GT":
			if nx || xx || gt || lt {
				return false, false, false, false, nil, fmt.Errorf("ERR Multiple condition flags specified")
			}
			gt = true
			idx++
		case "LT":
			if nx || xx || gt || lt {
				return false, false, false, false, nil, fmt.Errorf("ERR Multiple condition flags specified")
			}
			lt = true
			idx++
		case "FIELDS":
			return false, false, false, false, nil, fmt.Errorf("ERR FIELDS keyword specified multiple times")
		default:
			return false, false, false, false, nil, fmt.Errorf("ERR unknown argument '%s'", string(args[idx]))
		}
	}

	return nx, xx, gt, lt, fields, nil
}

// parseHTTLArgs parses arguments for HTTL/HPTTL/HEXPIRETIME/HPEXPIRETIME/HPERSIST
func parseHTTLArgs(args [][]byte) ([]string, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("wrong number of arguments")
	}

	// Must have FIELDS keyword
	if strings.ToUpper(string(args[1])) != "FIELDS" {
		return nil, fmt.Errorf("ERR unknown argument '%s'", string(args[1]))
	}

	numFields, ok := shared.ParseInt64(args[2])
	if !ok {
		return nil, fmt.Errorf("ERR value is not an integer or out of range")
	}
	if numFields <= 0 {
		return nil, fmt.Errorf("ERR Parameter `numFields` should be greater than 0")
	}

	// Must have at least numFields field names
	if int64(len(args)-3) < numFields {
		return nil, fmt.Errorf("wrong number of arguments")
	}

	fields := make([]string, numFields)
	for i := range numFields {
		fields[i] = string(args[3+int(i)])
	}

	// If there are extra arguments, they're unknown
	if int64(len(args)-3) > numFields {
		return nil, fmt.Errorf("ERR unknown argument '%s'", string(args[3+int(numFields)]))
	}

	return fields, nil
}

func handleHExpire(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 5 {
		w.WriteWrongNumArguments("hexpire")
		return
	}

	key := string(cmd.Args[0])
	seconds, ok := shared.ParseInt64(cmd.Args[1])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return
	}

	nx, xx, gt, lt, fields, err := parseHExpireArgs(cmd.Args, 2)
	if err != nil {
		w.WriteError(err.Error())
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		if !exists {
			w.WriteArray(len(fields))
			for range fields {
				w.WriteInteger(-2)
			}
			return
		}

		// Handle zero/negative seconds = delete the fields
		if seconds <= 0 {
			w.WriteArray(len(fields))
			for _, field := range fields {
				_, found := hashFieldExpiresAt(snap, field)
				if found {
					if err := emitHashRemove(cmd, key, []byte(field)); err != nil {
						w.WriteError(err.Error())
						return
					}
					w.WriteInteger(2) // Field deleted
				} else {
					w.WriteInteger(-2)
				}
			}
			return
		}

		expiresAt := time.Now().Add(time.Duration(seconds) * time.Second)

		handleExpiration(w, fields, snap, nx, xx, gt, expiresAt, lt, cmd, key)
	}
	return
}

func handleExpiration(w *shared.Writer, fields []string, snap *pb.ReducedEffect, nx bool, xx bool, gt bool, expiresAtMs time.Time, lt bool, cmd *shared.Command, key string) {
	w.WriteArray(len(fields))
	for _, field := range fields {
		currentExpiresAt, found := hashFieldExpiresAt(snap, field)
		if !found {
			w.WriteInteger(-2)
			continue
		}

		var zero time.Time

		// Check NX/XX/GT/LT conditions
		if nx && currentExpiresAt.After(zero) {
			w.WriteInteger(0)
			continue
		}
		if xx && currentExpiresAt == zero {
			w.WriteInteger(0)
			continue
		}
		if gt && expiresAtMs.Before(currentExpiresAt) {
			w.WriteInteger(0)
			continue
		}
		if lt && expiresAtMs.After(currentExpiresAt) && currentExpiresAt.After(zero) {
			w.WriteInteger(0)
			continue
		}

		if err := emitHashFieldTTL(cmd, key, field, expiresAtMs); err != nil {
			w.WriteError(err.Error())
			return
		}
		w.WriteInteger(1)
	}
}

func handleHPExpire(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 5 {
		w.WriteWrongNumArguments("hpexpire")
		return
	}

	key := string(cmd.Args[0])
	millis, ok := shared.ParseInt64(cmd.Args[1])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return
	}

	nx, xx, gt, lt, fields, err := parseHExpireArgs(cmd.Args, 2)
	if err != nil {
		w.WriteError(err.Error())
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		if !exists {
			w.WriteArray(len(fields))
			for range fields {
				w.WriteInteger(-2)
			}
			return
		}

		// Handle zero/negative millis = delete the fields
		if millis <= 0 {
			w.WriteArray(len(fields))
			for _, field := range fields {
				_, found := hashFieldExpiresAt(snap, field)
				if found {
					if err := emitHashRemove(cmd, key, []byte(field)); err != nil {
						w.WriteError(err.Error())
						return
					}
					w.WriteInteger(2)
				} else {
					w.WriteInteger(-2)
				}
			}
			return
		}

		expiresAt := time.Now().Add(time.Duration(millis) * time.Millisecond)

		handleExpiration(w, fields, snap, nx, xx, gt, expiresAt, lt, cmd, key)
	}
	return
}

func handleHExpireAt(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 5 {
		w.WriteWrongNumArguments("hexpireat")
		return
	}

	key := string(cmd.Args[0])
	unixSeconds, ok := shared.ParseInt64(cmd.Args[1])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return
	}

	nx, xx, gt, lt, fields, err := parseHExpireArgs(cmd.Args, 2)
	if err != nil {
		w.WriteError(err.Error())
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		if !exists {
			w.WriteArray(len(fields))
			for range fields {
				w.WriteInteger(-2)
			}
			return
		}

		expires := time.Unix(unixSeconds, 0)

		// Past timestamp: delete the fields (same as HEXPIRE with seconds <= 0)
		if expires.Before(time.Now()) {
			w.WriteArray(len(fields))
			for _, field := range fields {
				_, found := hashFieldExpiresAt(snap, field)
				if found {
					if err := emitHashRemove(cmd, key, []byte(field)); err != nil {
						w.WriteError(err.Error())
						return
					}
					w.WriteInteger(2) // Field deleted
				} else {
					w.WriteInteger(-2)
				}
			}
			return
		}

		handleExpiration(w, fields, snap, nx, xx, gt, expires, lt, cmd, key)
	}
	return
}

func handleHPExpireAt(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 5 {
		w.WriteWrongNumArguments("hpexpireat")
		return
	}

	key := string(cmd.Args[0])
	unixMillis, ok := shared.ParseInt64(cmd.Args[1])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return
	}

	nx, xx, gt, lt, fields, err := parseHExpireArgs(cmd.Args, 2)
	if err != nil {
		w.WriteError(err.Error())
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		if !exists {
			w.WriteArray(len(fields))
			for range fields {
				w.WriteInteger(-2)
			}
			return
		}

		expires := time.UnixMilli(unixMillis)

		// Past timestamp: delete the fields
		if expires.Before(time.Now()) {
			w.WriteArray(len(fields))
			for _, field := range fields {
				_, found := hashFieldExpiresAt(snap, field)
				if found {
					if err := emitHashRemove(cmd, key, []byte(field)); err != nil {
						w.WriteError(err.Error())
						return
					}
					w.WriteInteger(2)
				} else {
					w.WriteInteger(-2)
				}
			}
			return
		}

		handleExpiration(w, fields, snap, nx, xx, gt, expires, lt, cmd, key)
	}
	return
}

func handleHTTL(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("httl")
		return
	}

	key := string(cmd.Args[0])
	fields, err := parseHTTLArgs(cmd.Args)
	if err != nil {
		if err.Error() == "wrong number of arguments" {
			w.WriteWrongNumArguments("httl")
		} else {
			w.WriteError(err.Error())
		}
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		if !exists {
			w.WriteArray(len(fields))
			for range fields {
				w.WriteInteger(-2)
			}
			return
		}

		var zero time.Time

		w.WriteArray(len(fields))
		for _, field := range fields {
			expiresAt, found := hashFieldExpiresAt(snap, field)
			if !found {
				w.WriteInteger(-2)
			} else if expiresAt == zero {
				w.WriteInteger(-1)
			} else {
				remaining := expiresAt.Sub(time.Now())
				w.WriteInteger(remaining.Milliseconds())
			}
		}
	}
	return
}

func handleHPTTL(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("hpttl")
		return
	}

	key := string(cmd.Args[0])
	fields, err := parseHTTLArgs(cmd.Args)
	if err != nil {
		if err.Error() == "wrong number of arguments" {
			w.WriteWrongNumArguments("hpttl")
		} else {
			w.WriteError(err.Error())
		}
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		if !exists {
			w.WriteArray(len(fields))
			for range fields {
				w.WriteInteger(-2)
			}
			return
		}

		var zero time.Time

		w.WriteArray(len(fields))
		for _, field := range fields {
			expiresAt, found := hashFieldExpiresAt(snap, field)
			if !found {
				w.WriteInteger(-2)
			} else if expiresAt == zero {
				w.WriteInteger(-1)
			} else {
				remaining := expiresAt.Sub(time.Now())
				w.WriteInteger(remaining.Milliseconds())
			}
		}
	}
	return
}

func handleHExpireTime(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("hexpiretime")
		return
	}

	key := string(cmd.Args[0])
	fields, err := parseHTTLArgs(cmd.Args)
	if err != nil {
		if err.Error() == "wrong number of arguments" {
			w.WriteWrongNumArguments("hexpiretime")
		} else {
			w.WriteError(err.Error())
		}
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		if !exists {
			w.WriteArray(len(fields))
			for range fields {
				w.WriteInteger(-2)
			}
			return
		}

		var zero time.Time

		w.WriteArray(len(fields))
		for _, field := range fields {
			expiresAt, found := hashFieldExpiresAt(snap, field)
			if !found {
				w.WriteInteger(-2)
			} else if expiresAt == zero {
				w.WriteInteger(-1)
			} else {
				// Convert to seconds
				w.WriteInteger(expiresAt.Unix())
			}
		}
	}
	return
}

func handleHPExpireTime(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("hpexpiretime")
		return
	}

	key := string(cmd.Args[0])
	fields, err := parseHTTLArgs(cmd.Args)
	if err != nil {
		if err.Error() == "wrong number of arguments" {
			w.WriteWrongNumArguments("hpexpiretime")
		} else {
			w.WriteError(err.Error())
		}
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		if !exists {
			w.WriteArray(len(fields))
			for range fields {
				w.WriteInteger(-2)
			}
			return
		}

		var zero time.Time

		w.WriteArray(len(fields))
		for _, field := range fields {
			expiresAt, found := hashFieldExpiresAt(snap, field)
			if !found {
				w.WriteInteger(-2)
			} else if expiresAt == zero {
				w.WriteInteger(-1)
			} else {
				w.WriteInteger(expiresAt.UnixMilli())
			}
		}
	}
	return
}

func handleHPersist(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("hpersist")
		return
	}

	key := string(cmd.Args[0])
	fields, err := parseHTTLArgs(cmd.Args)
	if err != nil {
		if err.Error() == "wrong number of arguments" {
			w.WriteWrongNumArguments("hpersist")
		} else {
			w.WriteError(err.Error())
		}
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		if !exists {
			w.WriteArray(len(fields))
			for range fields {
				w.WriteInteger(-2)
			}
			return
		}

		var zero time.Time

		w.WriteArray(len(fields))
		for _, field := range fields {
			expiresAt, found := hashFieldExpiresAt(snap, field)
			if !found {
				w.WriteInteger(-2)
			} else if expiresAt == zero {
				// Field exists but has no TTL
				w.WriteInteger(-1)
			} else {
				// Field has TTL, persist it
				if err := emitHashFieldPersist(cmd, key, field); err != nil {
					w.WriteError(err.Error())
					return
				}
				w.WriteInteger(1)
			}
		}
	}
	return
}

func handleHSetEx(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	// HSETEX key [FNX | FXX] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL] FIELDS numfields field value [field value ...]
	// Options can appear before or after FIELDS section
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("hsetex")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	var fnx, fxx, keepTTL bool
	var expireMode string // "EX", "PX", "EXAT", "PXAT", or ""
	var expireValue int64 // value for expireMode
	var fieldValues [][]byte
	var numFields int64

	i := 1
	for i < len(cmd.Args) {
		arg := strings.ToUpper(string(cmd.Args[i]))

		switch arg {
		case "FNX":
			if fnx || fxx {
				w.WriteError("ERR Only one of FXX or FNX arguments can be specified for HSETEX")
				return
			}
			fnx = true
			i++
		case "FXX":
			if fnx || fxx {
				w.WriteError("ERR Only one of FXX or FNX arguments can be specified for HSETEX")
				return
			}
			fxx = true
			i++
		case "EX":
			if expireMode != "" || keepTTL {
				w.WriteError("ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified for HSETEX")
				return
			}
			if i+1 >= len(cmd.Args) {
				w.WriteWrongNumArguments("hsetex")
				return
			}
			seconds, ok := shared.ParseInt64(cmd.Args[i+1])
			if !ok || seconds <= 0 {
				w.WriteError("ERR invalid expire time in 'hsetex' command")
				return
			}
			expireMode = "EX"
			expireValue = seconds
			i += 2
		case "PX":
			if expireMode != "" || keepTTL {
				w.WriteError("ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified for HSETEX")
				return
			}
			if i+1 >= len(cmd.Args) {
				w.WriteWrongNumArguments("hsetex")
				return
			}
			ms, ok := shared.ParseInt64(cmd.Args[i+1])
			if !ok || ms <= 0 {
				w.WriteError("ERR invalid expire time in 'hsetex' command")
				return
			}
			expireMode = "PX"
			expireValue = ms
			i += 2
		case "EXAT":
			if expireMode != "" || keepTTL {
				w.WriteError("ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified for HSETEX")
				return
			}
			if i+1 >= len(cmd.Args) {
				w.WriteWrongNumArguments("hsetex")
				return
			}
			seconds, ok := shared.ParseInt64(cmd.Args[i+1])
			if !ok || seconds <= 0 {
				w.WriteError("ERR invalid expire time in 'hsetex' command")
				return
			}
			expireMode = "EXAT"
			expireValue = seconds
			i += 2
		case "PXAT":
			if expireMode != "" || keepTTL {
				w.WriteError("ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified for HSETEX")
				return
			}
			if i+1 >= len(cmd.Args) {
				w.WriteWrongNumArguments("hsetex")
				return
			}
			ms, ok := shared.ParseInt64(cmd.Args[i+1])
			if !ok || ms <= 0 {
				w.WriteError("ERR invalid expire time in 'hsetex' command")
				return
			}
			expireMode = "PXAT"
			expireValue = ms
			i += 2
		case "KEEPTTL":
			if expireMode != "" || keepTTL {
				w.WriteError("ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified for HSETEX")
				return
			}
			keepTTL = true
			i++
		case "FIELDS":
			// Parse numfields
			if i+1 >= len(cmd.Args) {
				w.WriteWrongNumArguments("hsetex")
				return
			}
			var ok bool
			numFields, ok = shared.ParseInt64(cmd.Args[i+1])
			if !ok {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			if numFields <= 0 {
				w.WriteError("ERR invalid number of fields")
				return
			}

			// Check we have enough args for field-value pairs
			fieldValuesStart := i + 2
			fieldValuesEnd := fieldValuesStart + int(numFields)*2
			if fieldValuesEnd > len(cmd.Args) {
				w.WriteWrongNumArguments("hsetex")
				return
			}

			// Parse field-value pairs
			fieldValues = make([][]byte, numFields*2)
			for j := int64(0); j < numFields; j++ {
				fieldValues[j*2] = cmd.Args[fieldValuesStart+int(j)*2]
				fieldValues[j*2+1] = cmd.Args[fieldValuesStart+int(j)*2+1]
			}
			i = fieldValuesEnd
		default:
			w.WriteError("ERR unknown argument '" + string(cmd.Args[i]) + "'")
			return
		}
	}

	// Check that FIELDS was provided
	if fieldValues == nil {
		w.WriteWrongNumArguments("hsetex")
		return
	}

	valid = true
	runner = func() {
		// Compute expireAt based on mode
		var expiresAtMs uint64
		switch expireMode {
		case "EX":
			expiresAtMs = uint64(time.Now().UnixMilli() + expireValue*1000)
		case "PX":
			expiresAtMs = uint64(time.Now().UnixMilli() + expireValue)
		case "EXAT":
			expiresAtMs = uint64(expireValue * 1000)
		case "PXAT":
			expiresAtMs = uint64(expireValue)
		}

		expiresAt := time.UnixMilli(int64(expiresAtMs))

		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		// Check FNX/FXX conditions
		if fnx || fxx {
			if !exists {
				if fxx {
					// FXX requires all fields to exist, but hash doesn't exist
					w.WriteInteger(0)
					return
				}
				// FNX: hash doesn't exist, all fields are new, proceed
			} else {
				// Hash exists, check fields
				allExist := true
				noneExist := true
				for j := int64(0); j < numFields; j++ {
					field := string(fieldValues[j*2])
					if snap != nil && snap.NetAdds != nil {
						if _, has := snap.NetAdds[field]; has {
							noneExist = false
						} else {
							allExist = false
						}
					} else {
						allExist = false
					}
				}

				if fnx && !noneExist {
					w.WriteInteger(0)
					return
				}
				if fxx && !allExist {
					w.WriteInteger(0)
					return
				}
			}
		}

		// Emit inserts for each field
		for j := int64(0); j < numFields; j++ {
			field := fieldValues[j*2]
			value := make([]byte, len(fieldValues[j*2+1]))
			copy(value, fieldValues[j*2+1])

			if err := emitHashInsert(cmd, key, field, value); err != nil {
				w.WriteError(err.Error())
				return
			}

			// Set TTL per field
			if !keepTTL && expiresAtMs > 0 {
				if err := emitHashFieldTTL(cmd, key, string(field), expiresAt); err != nil {
					w.WriteError(err.Error())
					return
				}
			}
		}

		// Emit type tag if key is new
		if !exists {
			if err := emitHashTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteInteger(1)
	}
	return
}

func handleHGetEx(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	// HGETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST] FIELDS numfields field [field ...]
	// Options can appear before or after FIELDS section
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("hgetex")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	var persist bool
	var expireMode string // "EX", "PX", "EXAT", "PXAT", or ""
	var expireValue int64 // value for expireMode
	var fields []string
	var numFields int64

	i := 1
	for i < len(cmd.Args) {
		arg := strings.ToUpper(string(cmd.Args[i]))

		switch arg {
		case "EX":
			if expireMode != "" || persist {
				w.WriteError("ERR Only one of EX, PX, EXAT, PXAT or PERSIST arguments can be specified for HGETEX")
				return
			}
			if i+1 >= len(cmd.Args) {
				w.WriteWrongNumArguments("hgetex")
				return
			}
			seconds, ok := shared.ParseInt64(cmd.Args[i+1])
			if !ok {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			if seconds < 0 {
				w.WriteError("ERR invalid expire time, must be >= 0")
				return
			}
			if seconds == 0 || seconds > (1<<46)/1000 {
				w.WriteError("ERR invalid expire time in 'hgetex' command")
				return
			}
			expireMode = "EX"
			expireValue = seconds
			i += 2
		case "PX":
			if expireMode != "" || persist {
				w.WriteError("ERR Only one of EX, PX, EXAT, PXAT or PERSIST arguments can be specified for HGETEX")
				return
			}
			if i+1 >= len(cmd.Args) {
				w.WriteWrongNumArguments("hgetex")
				return
			}
			ms, ok := shared.ParseInt64(cmd.Args[i+1])
			if !ok {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			if ms < 0 {
				w.WriteError("ERR invalid expire time, must be >= 0")
				return
			}
			if ms == 0 || ms > (1<<46) {
				w.WriteError("ERR invalid expire time in 'hgetex' command")
				return
			}
			expireMode = "PX"
			expireValue = ms
			i += 2
		case "EXAT":
			if expireMode != "" || persist {
				w.WriteError("ERR Only one of EX, PX, EXAT, PXAT or PERSIST arguments can be specified for HGETEX")
				return
			}
			if i+1 >= len(cmd.Args) {
				w.WriteWrongNumArguments("hgetex")
				return
			}
			seconds, ok := shared.ParseInt64(cmd.Args[i+1])
			if !ok {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			if seconds < 0 {
				w.WriteError("ERR invalid expire time, must be >= 0")
				return
			}
			if seconds == 0 || seconds > (1<<46)/1000 {
				w.WriteError("ERR invalid expire time in 'hgetex' command")
				return
			}
			expireMode = "EXAT"
			expireValue = seconds
			i += 2
		case "PXAT":
			if expireMode != "" || persist {
				w.WriteError("ERR Only one of EX, PX, EXAT, PXAT or PERSIST arguments can be specified for HGETEX")
				return
			}
			if i+1 >= len(cmd.Args) {
				w.WriteWrongNumArguments("hgetex")
				return
			}
			ms, ok := shared.ParseInt64(cmd.Args[i+1])
			if !ok {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			if ms < 0 {
				w.WriteError("ERR invalid expire time, must be >= 0")
				return
			}
			if ms == 0 || ms > (1<<46) {
				w.WriteError("ERR invalid expire time in 'hgetex' command")
				return
			}
			expireMode = "PXAT"
			expireValue = ms
			i += 2
		case "PERSIST":
			if expireMode != "" || persist {
				w.WriteError("ERR Only one of EX, PX, EXAT, PXAT or PERSIST arguments can be specified for HGETEX")
				return
			}
			persist = true
			i++
		case "FIELDS":
			// Parse numfields
			if i+1 >= len(cmd.Args) {
				w.WriteWrongNumArguments("hgetex")
				return
			}
			var ok bool
			numFields, ok = shared.ParseInt64(cmd.Args[i+1])
			if !ok {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			if numFields <= 0 {
				w.WriteError("ERR invalid number of fields")
				return
			}

			// Check we have enough args for field names
			fieldNamesStart := i + 2
			fieldNamesEnd := fieldNamesStart + int(numFields)
			if fieldNamesEnd > len(cmd.Args) {
				w.WriteWrongNumArguments("hgetex")
				return
			}

			// Parse field names
			fields = make([]string, numFields)
			for j := int64(0); j < numFields; j++ {
				fields[j] = string(cmd.Args[fieldNamesStart+int(j)])
			}
			i = fieldNamesEnd
		default:
			w.WriteError("ERR unknown argument '" + string(cmd.Args[i]) + "'")
			return
		}
	}

	// Check that FIELDS was provided
	if fields == nil {
		w.WriteWrongNumArguments("hgetex")
		return
	}

	valid = true
	runner = func() {
		// Compute expireAt based on mode
		var expiresAtMs uint64
		switch expireMode {
		case "EX":
			expiresAtMs = uint64(time.Now().UnixMilli() + expireValue*1000)
		case "PX":
			expiresAtMs = uint64(time.Now().UnixMilli() + expireValue)
		case "EXAT":
			expiresAtMs = uint64(expireValue * 1000)
		case "PXAT":
			expiresAtMs = uint64(expireValue)
		}

		expiresAt := time.UnixMilli(int64(expiresAtMs))

		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		if !exists {
			w.WriteArray(int(numFields))
			for range numFields {
				w.WriteNullBulkString()
			}
			return
		}

		// Get values and optionally set TTLs
		w.WriteArray(int(numFields))
		for _, field := range fields {
			val, fieldExists := hashFieldValue(snap, field)
			if !fieldExists {
				w.WriteNullBulkString()
			} else {
				w.WriteBulkString(val)
				// Set TTL if requested
				if expiresAtMs > 0 {
					if err := emitHashFieldTTL(cmd, key, field, expiresAt); err != nil {
						w.WriteError(err.Error())
						return
					}
				} else if persist {
					if err := emitHashFieldPersist(cmd, key, field); err != nil {
						w.WriteError(err.Error())
						return
					}
				}
			}
		}
	}
	return
}
