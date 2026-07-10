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

package redis

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/swytchdb/swytch/redis/shared"
)

// handleAcl dispatches ACL subcommands
func (h *Handler) handleAcl(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	if len(cmd.Args) == 0 {
		w.WriteError("ERR wrong number of arguments for 'acl' command")
		return
	}

	subCmd := strings.ToUpper(string(cmd.Args[0]))
	switch subCmd {
	case "SETUSER":
		h.handleAclSetUser(cmd, w, conn)
	case "GETUSER":
		h.handleAclGetUser(cmd, w, conn)
	case "DELUSER":
		h.handleAclDelUser(cmd, w, conn)
	case "LIST":
		h.handleAclList(cmd, w, conn)
	case "USERS":
		h.handleAclUsers(cmd, w, conn)
	case "CAT":
		h.handleAclCat(cmd, w)
	case "WHOAMI":
		h.handleAclWhoami(cmd, w, conn)
	case "GENPASS":
		h.handleAclGenpass(cmd, w)
	case "LOAD":
		h.handleAclLoad(cmd, w)
	case "SAVE":
		h.handleAclSave(cmd, w)
	case "LOG":
		h.handleAclLog(cmd, w)
	case "DRYRUN":
		h.handleAclDryrun(cmd, w, conn)
	case "HELP":
		h.handleAclHelp(cmd, w)
	default:
		w.WriteError(fmt.Sprintf("ERR unknown subcommand '%s'. Try ACL HELP.", subCmd))
	}
}

// handleAclSetUser handles ACL SETUSER username [rule ...]
func (h *Handler) handleAclSetUser(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	if len(cmd.Args) < 2 {
		w.WriteError("ERR wrong number of arguments for 'acl|setuser' command")
		return
	}

	username := string(cmd.Args[1])

	h.aclManager.mu.Lock()
	user, exists := h.aclManager.users[username]
	if !exists {
		// Create new user with default disabled state
		user = &shared.ACLUser{
			Name:        username,
			Enabled:     false,
			Categories:  make(map[shared.CommandCategory]bool),
			Commands:    make(map[shared.CommandType]bool),
			Subcommands: make(map[shared.CommandType]map[string]bool),
		}
		h.aclManager.users[username] = user
	}
	h.aclManager.mu.Unlock()

	// Parse and apply rules
	rules := make([]string, len(cmd.Args)-2)
	for i, arg := range cmd.Args[2:] {
		rules[i] = string(arg)
	}

	if err := h.aclManager.applyACLRules(user, rules); err != nil {
		w.WriteError(fmt.Sprintf("ERR %v", err))
		return
	}

	w.WriteOK()
}

// handleAclGetUser handles ACL GETUSER username
func (h *Handler) handleAclGetUser(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	if len(cmd.Args) != 2 {
		w.WriteError("ERR wrong number of arguments for 'acl|getuser' command")
		return
	}

	username := string(cmd.Args[1])
	user := h.aclManager.GetUser(username)
	if user == nil {
		w.WriteNullBulkString()
		return
	}

	// Return user info as a map
	info := user.GetUserForDisplay()

	if conn.Protocol == shared.RESP3 {
		// RESP3: return as a map
		w.WriteMap(5)

		w.WriteBulkStringStr("flags")
		flags := info["flags"].([]string)
		w.WriteArray(len(flags))
		for _, f := range flags {
			w.WriteBulkStringStr(f)
		}

		w.WriteBulkStringStr("passwords")
		passwords := info["passwords"].([]string)
		w.WriteArray(len(passwords))
		for _, p := range passwords {
			w.WriteBulkStringStr(p)
		}

		w.WriteBulkStringStr("commands")
		w.WriteBulkStringStr(info["commands"].(string))

		w.WriteBulkStringStr("keys")
		keys := info["keys"].([]string)
		w.WriteArray(len(keys))
		for _, k := range keys {
			w.WriteBulkStringStr(k)
		}

		w.WriteBulkStringStr("channels")
		channels := info["channels"].([]string)
		w.WriteArray(len(channels))
		for _, c := range channels {
			w.WriteBulkStringStr(c)
		}
	} else {
		// RESP2: return as flat array
		w.WriteArray(10)

		w.WriteBulkStringStr("flags")
		flags := info["flags"].([]string)
		w.WriteArray(len(flags))
		for _, f := range flags {
			w.WriteBulkStringStr(f)
		}

		w.WriteBulkStringStr("passwords")
		passwords := info["passwords"].([]string)
		w.WriteArray(len(passwords))
		for _, p := range passwords {
			w.WriteBulkStringStr(p)
		}

		w.WriteBulkStringStr("commands")
		w.WriteBulkStringStr(info["commands"].(string))

		w.WriteBulkStringStr("keys")
		keys := info["keys"].([]string)
		w.WriteArray(len(keys))
		for _, k := range keys {
			w.WriteBulkStringStr(k)
		}

		w.WriteBulkStringStr("channels")
		channels := info["channels"].([]string)
		w.WriteArray(len(channels))
		for _, c := range channels {
			w.WriteBulkStringStr(c)
		}
	}
}

// handleAclDelUser handles ACL DELUSER username [username ...]
func (h *Handler) handleAclDelUser(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	if len(cmd.Args) < 2 {
		w.WriteError("ERR wrong number of arguments for 'acl|deluser' command")
		return
	}

	count := 0
	for _, arg := range cmd.Args[1:] {
		username := string(arg)
		if username == "default" {
			// Cannot delete default user
			continue
		}
		if h.aclManager.DeleteUser(username) {
			count++
		}
	}

	w.WriteInteger(int64(count))
}

// handleAclList handles ACL LIST
func (h *Handler) handleAclList(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	users := h.aclManager.GetUsers()
	sort.Strings(users)

	w.WriteArray(len(users))
	for _, username := range users {
		user := h.aclManager.GetUser(username)
		if user != nil {
			rule := h.aclManager.formatUserRule(user)
			w.WriteBulkStringStr(rule)
		}
	}
}

// handleAclUsers handles ACL USERS
func (h *Handler) handleAclUsers(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	users := h.aclManager.GetUsers()
	sort.Strings(users)

	w.WriteArray(len(users))
	for _, username := range users {
		w.WriteBulkStringStr(username)
	}
}

// handleAclCat handles ACL CAT [category]
func (h *Handler) handleAclCat(cmd *shared.Command, w *shared.Writer) {
	if len(cmd.Args) == 1 {
		// List all categories
		names := shared.GetAllCategoryNames()
		sort.Strings(names)
		w.WriteArray(len(names))
		for _, name := range names {
			w.WriteBulkStringStr(name)
		}
		return
	}

	if len(cmd.Args) == 2 {
		// List commands in category
		catName := strings.ToLower(string(cmd.Args[1]))
		cat, ok := shared.GetCategoryByName(catName)
		if !ok {
			w.WriteError(fmt.Sprintf("ERR unknown category '%s'", catName))
			return
		}

		cmds := shared.GetCommandsInCategory(cat)
		w.WriteArray(len(cmds))
		for _, c := range cmds {
			w.WriteBulkStringStr(c.String())
		}
		return
	}

	w.WriteError("ERR wrong number of arguments for 'acl|cat' command")
}

// handleAclWhoami handles ACL WHOAMI
func (h *Handler) handleAclWhoami(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	if conn.User == nil {
		w.WriteBulkStringStr("default")
	} else {
		w.WriteBulkStringStr(conn.User.Name)
	}
}

// handleAclGenpass handles ACL GENPASS [bits]
func (h *Handler) handleAclGenpass(cmd *shared.Command, w *shared.Writer) {
	bits := 256 // default
	if len(cmd.Args) >= 2 {
		b, err := strconv.Atoi(string(cmd.Args[1]))
		if err != nil || b <= 0 || b > 8192 {
			w.WriteError("ERR ACL GENPASS bits must be between 1 and 8192")
			return
		}
		bits = b
	}

	pass, err := GeneratePassword(bits)
	if err != nil {
		w.WriteError(fmt.Sprintf("ERR failed to generate password: %v", err))
		return
	}

	w.WriteBulkStringStr(pass)
}

// handleAclLoad handles ACL LOAD
func (h *Handler) handleAclLoad(cmd *shared.Command, w *shared.Writer) {
	if h.aclManager.aclFile == "" {
		w.WriteError("ERR This Redis instance is not configured to use an ACL file. You may want to specify users via the ACL SETUSER command and then issue a CONFIG REWRITE (if you have a Redis.conf) to store users in the Redis config file.")
		return
	}

	if err := h.aclManager.LoadFromFile(h.aclManager.aclFile); err != nil {
		w.WriteError(fmt.Sprintf("ERR Error loading ACLs: %v", err))
		return
	}

	w.WriteOK()
}

// handleAclSave handles ACL SAVE
func (h *Handler) handleAclSave(cmd *shared.Command, w *shared.Writer) {
	if h.aclManager.aclFile == "" {
		w.WriteError("ERR This Redis instance is not configured to use an ACL file. You may want to specify users via the ACL SETUSER command and then issue a CONFIG REWRITE (if you have a Redis.conf) to store users in the Redis config file.")
		return
	}

	if err := h.aclManager.SaveToFile(""); err != nil {
		w.WriteError(fmt.Sprintf("ERR Error saving ACLs: %v", err))
		return
	}

	w.WriteOK()
}

// handleAclLog handles ACL LOG [count|RESET]
func (h *Handler) handleAclLog(cmd *shared.Command, w *shared.Writer) {
	if len(cmd.Args) >= 2 {
		arg := strings.ToUpper(string(cmd.Args[1]))
		if arg == "RESET" {
			h.aclManager.ResetLog()
			w.WriteOK()
			return
		}

		count, err := strconv.Atoi(string(cmd.Args[1]))
		if err != nil {
			w.WriteError("ERR invalid count argument")
			return
		}

		entries := h.aclManager.GetLog(count)
		h.writeACLLogEntries(w, entries)
		return
	}

	// Default: return up to 10 entries
	entries := h.aclManager.GetLog(10)
	h.writeACLLogEntries(w, entries)
}

func (h *Handler) writeACLLogEntries(w *shared.Writer, entries []ACLLogEntry) {
	w.WriteArray(len(entries))
	for _, entry := range entries {
		w.WriteMap(8)

		w.WriteBulkStringStr("count")
		w.WriteInteger(int64(entry.Count))

		w.WriteBulkStringStr("reason")
		w.WriteBulkStringStr(entry.Reason)

		w.WriteBulkStringStr("context")
		w.WriteBulkStringStr(entry.Context)

		w.WriteBulkStringStr("object")
		w.WriteBulkStringStr(entry.Object)

		w.WriteBulkStringStr("username")
		w.WriteBulkStringStr(entry.Username)

		w.WriteBulkStringStr("age-seconds")
		w.WriteBulkStringStr(fmt.Sprintf("%.3f", entry.AgeSeconds))

		w.WriteBulkStringStr("client-info")
		w.WriteBulkStringStr(entry.ClientInfo)

		w.WriteBulkStringStr("entry-id")
		w.WriteInteger(entry.EntryID)
	}
}

// handleAclDryrun handles ACL DRYRUN username command [arg ...]
func (h *Handler) handleAclDryrun(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	if len(cmd.Args) < 3 {
		w.WriteError("ERR wrong number of arguments for 'acl|dryrun' command")
		return
	}

	username := string(cmd.Args[1])
	user := h.aclManager.GetUser(username)
	if user == nil {
		w.WriteError(fmt.Sprintf("ERR User '%s' not found", username))
		return
	}

	// Parse the command
	cmdName := strings.ToUpper(string(cmd.Args[2]))
	cmdType, ok := shared.LookupCommandName(cmdName)
	if !ok {
		w.WriteError(fmt.Sprintf("ERR unknown command '%s'", cmdName))
		return
	}

	// Check command permission first
	if err := h.aclManager.CheckCommand(user, cmdType); err != nil {
		w.WriteBulkStringStr(err.Error())
		return
	}

	// Build a Command object to extract keys using the handler's validation
	testCmd := &shared.Command{
		Type: cmdType,
		Args: cmd.Args[3:], // Arguments after the command name
	}

	// Extract keys using the handler's validation phase
	keys := h.extractKeysForAcl(testCmd, conn)

	// Check key permissions
	isWrite := false
	if e := h.cmds.Lookup(cmdType); e != nil {
		isWrite = e.Flags&shared.FlagWrite != 0
	}
	for _, key := range keys {
		if err := h.aclManager.CheckKey(user, key, isWrite); err != nil {
			w.WriteBulkStringStr(err.Error())
			return
		}
	}

	w.WriteOK()
}

// extractKeysForAcl extracts keys from a command using the handler's validation phase.
// This is used by ACL DRYRUN to properly identify which arguments are keys.
func (h *Handler) extractKeysForAcl(cmd *shared.Command, conn *shared.Connection) []string {
	// Create a discard buffer for the null writer
	var discardBuf bytes.Buffer
	nullWriter := shared.NewWriter(&discardBuf)
	db := h.dbManager.DB()

	var keys []string

	// Handle scripting commands specially (they need Connection)
	switch cmd.Type {
	case shared.CmdEval, shared.CmdEvalSha, shared.CmdEvalRO, shared.CmdEvalShaRO, shared.CmdFcall, shared.CmdFcallRO:
		return shared.KeysNumkeysAtOffset(cmd, 1)
	}

	// Call the handler's validation phase to extract keys
	// The handlers return (valid, keys, runner) - we only care about keys
	_, extractedKeys, _ := h.dispatchForKeyExtraction(cmd, nullWriter, db, conn)

	keys = extractedKeys
	return keys
}

// dispatchForKeyExtraction calls the appropriate handler to extract keys.
// This mirrors the dispatch in ExecuteInto but only for key extraction.
func (h *Handler) dispatchForKeyExtraction(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) (valid bool, keys []string, runner shared.CommandRunner) {
	entry := h.cmds.Lookup(cmd.Type)
	if entry == nil {
		return false, nil, nil
	}

	// Use standard Handler if available (returns keys directly)
	if entry.Handler != nil {
		return entry.Handler(cmd, w, db)
	}

	// For ConnHandler-only commands (e.g. XREAD, XREADGROUP), use the Keys extractor
	if entry.Keys != nil {
		return true, entry.Keys(cmd), nil
	}

	return false, nil, nil
}

// handleAclHelp handles ACL HELP
func (h *Handler) handleAclHelp(cmd *shared.Command, w *shared.Writer) {
	help := []string{
		"ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
		"CAT [<category>]",
		"    List all commands in a category, or all categories if none given.",
		"DELUSER <username> [<username> ...]",
		"    Delete the specified users. The default user cannot be deleted.",
		"DRYRUN <username> <command> [<arg> ...]",
		"    Validate that the user can execute the command without running it.",
		"GENPASS [<bits>]",
		"    Generate a random password of the specified length (default: 256 bits).",
		"GETUSER <username>",
		"    Get the ACL rules for a user.",
		"LIST",
		"    List all users and their ACL rules.",
		"LOAD",
		"    Reload users from the ACL file.",
		"LOG [<count>|RESET]",
		"    Show or reset the ACL log entries.",
		"SAVE",
		"    Save users to the ACL file.",
		"SETUSER <username> [rule [rule ...]]",
		"    Create or modify a user with the specified rules.",
		"USERS",
		"    List all usernames.",
		"WHOAMI",
		"    Get the username of the current connection.",
		"HELP",
		"    Print this help message.",
	}

	w.WriteArray(len(help))
	for _, line := range help {
		w.WriteBulkStringStr(line)
	}
}

// registerAclCommands registers ACL commands.
func (h *Handler) registerAclCommands() {
	h.cmds.Register(shared.CmdAcl, &shared.CommandEntry{
		ConnHandler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, conn *shared.Connection) {
			h.handleAcl(cmd, w, conn)
		},
	})
}
