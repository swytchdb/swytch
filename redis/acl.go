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
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/swytchdb/swytch/redis/shared"
	"golang.org/x/crypto/bcrypt"
)

// ACL errors
var (
	ErrUserNotFound     = errors.New("ERR User not found")
	ErrUserDisabled     = errors.New("WRONGPASS invalid username-password pair or user is disabled")
	ErrWrongPassword    = errors.New("WRONGPASS invalid username-password pair or user is disabled")
	ErrNoPermission     = errors.New("NOPERM this user has no permissions to run the command")
	ErrNoKeyPermission  = errors.New("NOPERM this user has no permissions to access one of the keys")
	ErrNoChanPermission = errors.New("NOPERM this user has no permissions to access one of the channels")
)

// ACLLogEntry represents an entry in the ACL security log
type ACLLogEntry struct {
	Count         int     // Number of times this event occurred
	Reason        string  // "command", "key", "channel", "auth"
	Context       string  // "toplevel", "multi", "lua"
	Object        string  // The command or key that was denied
	Username      string  // The user that was denied
	AgeSeconds    float64 // Seconds since this entry was created
	ClientInfo    string  // Client connection info
	EntryID       int64   // Unique ID for deduplication
	TimestampUsec int64   // Microseconds since epoch
}

// ACLManager manages Redis ACL users and permissions
type ACLManager struct {
	mu        xsync.RBMutex
	users     map[string]*shared.ACLUser
	aclFile   string
	logMu     sync.Mutex
	log       []ACLLogEntry
	logMaxLen int
	nextLogID int64
}

// NewACLManager creates a new ACL manager
func NewACLManager() *ACLManager {
	m := &ACLManager{
		users:     make(map[string]*shared.ACLUser),
		logMaxLen: 128,
	}
	// Create default user with nopass and all permissions
	m.users["default"] = &shared.ACLUser{
		Name:        "default",
		Enabled:     true,
		NoPass:      true,
		AllCommands: true,
		AllKeys:     true,
		AllChannels: true,
		Categories:  make(map[shared.CommandCategory]bool),
		Commands:    make(map[shared.CommandType]bool),
		Subcommands: make(map[shared.CommandType]map[string]bool),
	}
	return m
}

// GetUser returns a user by name (thread-safe)
func (m *ACLManager) GetUser(name string) *shared.ACLUser {
	token := m.mu.RLock()
	defer m.mu.RUnlock(token)
	return m.users[name]
}

// GetUsers returns a copy of all user names (thread-safe)
func (m *ACLManager) GetUsers() []string {
	token := m.mu.RLock()
	defer m.mu.RUnlock(token)
	names := make([]string, 0, len(m.users))
	for name := range m.users {
		names = append(names, name)
	}
	return names
}

// SetUser creates or updates a user (thread-safe)
func (m *ACLManager) SetUser(user *shared.ACLUser) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.users[user.Name] = user
}

// DeleteUser removes a user (thread-safe)
func (m *ACLManager) DeleteUser(name string) bool {
	if name == "default" {
		return false // Cannot delete default user
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.users[name]; ok {
		delete(m.users, name)
		return true
	}
	return false
}

// Authenticate validates username and password, returns the user if successful
func (m *ACLManager) Authenticate(username, password string) (*shared.ACLUser, error) {
	token := m.mu.RLock()
	user, ok := m.users[username]
	m.mu.RUnlock(token)

	if !ok {
		return nil, ErrUserNotFound
	}

	if !user.Enabled {
		return nil, ErrUserDisabled
	}

	if user.NoPass {
		return user, nil
	}

	// Check password against all hashes
	for _, hash := range user.PasswordHashes {
		if err := bcrypt.CompareHashAndPassword(hash, []byte(password)); err == nil {
			return user, nil
		}
	}

	return nil, ErrWrongPassword
}

// AuthenticateDefault returns the default user if it has nopass, otherwise requires auth
func (m *ACLManager) AuthenticateDefault() (*shared.ACLUser, bool) {
	token := m.mu.RLock()
	user, ok := m.users["default"]
	m.mu.RUnlock(token)

	if !ok || !user.Enabled {
		return nil, false
	}

	if user.NoPass {
		return user, true
	}

	return nil, false
}

// RequiresAuth returns true if authentication is required
func (m *ACLManager) RequiresAuth() bool {
	token := m.mu.RLock()
	defer m.mu.RUnlock(token)
	user, ok := m.users["default"]
	if !ok || !user.Enabled {
		return true
	}
	return !user.NoPass
}

// CheckCommand checks if a user has permission to execute a command
func (m *ACLManager) CheckCommand(user *shared.ACLUser, cmd shared.CommandType) error {
	if user == nil {
		return ErrNoPermission
	}

	if !user.Enabled {
		return ErrUserDisabled
	}

	// Fast path: all commands allowed
	if user.AllCommands {
		return nil
	}

	// Check explicit command permissions (highest priority)
	if allowed, ok := user.Commands[cmd]; ok {
		if allowed {
			return nil
		}
		return ErrNoPermission
	}

	// Check category permissions
	cats := shared.GetCommandCategories(cmd)
	for _, cat := range cats {
		if allowed, ok := user.Categories[cat]; ok {
			if allowed {
				return nil
			}
			// Continue checking other categories, explicit deny doesn't stop us
		}
	}

	// Check if any category is allowed
	for _, cat := range cats {
		if user.Categories[cat] {
			return nil
		}
	}

	return ErrNoPermission
}

// CheckKey checks if a user has permission to access a key
func (m *ACLManager) CheckKey(user *shared.ACLUser, key string, isWrite bool) error {
	if user == nil {
		return ErrNoKeyPermission
	}

	// Fast path: all keys allowed
	if user.AllKeys {
		return nil
	}

	// Check key patterns
	for _, pattern := range user.KeyPatterns {
		if aclMatchGlob(pattern.Pattern, key) {
			switch pattern.Type {
			case shared.KeyPatternReadWrite:
				return nil
			case shared.KeyPatternReadOnly:
				if !isWrite {
					return nil
				}
			case shared.KeyPatternWriteOnly:
				if isWrite {
					return nil
				}
			}
		}
	}

	// Check selectors
	for _, selector := range user.Selectors {
		if selector.AllKeys {
			return nil
		}
		for _, pattern := range selector.KeyPatterns {
			if aclMatchGlob(pattern.Pattern, key) {
				switch pattern.Type {
				case shared.KeyPatternReadWrite:
					return nil
				case shared.KeyPatternReadOnly:
					if !isWrite {
						return nil
					}
				case shared.KeyPatternWriteOnly:
					if isWrite {
						return nil
					}
				}
			}
		}
	}

	return ErrNoKeyPermission
}

// CheckChannel checks if a user has permission to access a pub/sub channel
func (m *ACLManager) CheckChannel(user *shared.ACLUser, channel string) error {
	if user == nil {
		return ErrNoChanPermission
	}

	// Fast path: all channels allowed
	if user.AllChannels {
		return nil
	}

	// Check channel patterns
	for _, pattern := range user.ChannelPatterns {
		if aclMatchGlob(pattern, channel) {
			return nil
		}
	}

	return ErrNoChanPermission
}

// LogDenied logs a permission denial
func (m *ACLManager) LogDenied(reason, context, object, username, clientInfo string) {
	m.logMu.Lock()
	defer m.logMu.Unlock()

	now := time.Now()
	usec := now.UnixMicro()

	// Check for duplicate entry to increment count
	for i := range m.log {
		entry := &m.log[i]
		if entry.Reason == reason && entry.Object == object && entry.Username == username && entry.Context == context {
			entry.Count++
			entry.AgeSeconds = 0
			entry.TimestampUsec = usec
			return
		}
	}

	// Add new entry
	entry := ACLLogEntry{
		Count:         1,
		Reason:        reason,
		Context:       context,
		Object:        object,
		Username:      username,
		AgeSeconds:    0,
		ClientInfo:    clientInfo,
		EntryID:       m.nextLogID,
		TimestampUsec: usec,
	}
	m.nextLogID++

	m.log = append([]ACLLogEntry{entry}, m.log...)
	if len(m.log) > m.logMaxLen {
		m.log = m.log[:m.logMaxLen]
	}
}

// GetLog returns the ACL security log entries
func (m *ACLManager) GetLog(count int) []ACLLogEntry {
	m.logMu.Lock()
	defer m.logMu.Unlock()

	// Update age for all entries
	now := time.Now()
	for i := range m.log {
		m.log[i].AgeSeconds = float64(now.UnixMicro()-m.log[i].TimestampUsec) / 1e6
	}

	if count <= 0 || count > len(m.log) {
		count = len(m.log)
	}

	result := make([]ACLLogEntry, count)
	copy(result, m.log[:count])
	return result
}

// ResetLog clears the ACL security log
func (m *ACLManager) ResetLog() {
	m.logMu.Lock()
	defer m.logMu.Unlock()
	m.log = m.log[:0]
}

// HashPassword hashes a password using bcrypt
func HashPassword(password string) ([]byte, error) {
	return bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
}

// GeneratePassword generates a random password of the specified bit length
func GeneratePassword(bits int) (string, error) {
	if bits <= 0 {
		bits = 256
	}
	bytes := (bits + 7) / 8
	buf := make([]byte, bytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// LoadFromFile loads ACL rules from a file
func (m *ACLManager) LoadFromFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Clear existing users except default (we'll recreate it)
	m.users = make(map[string]*shared.ACLUser)

	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if err := m.parseACLLine(line); err != nil {
			return fmt.Errorf("line %d: %w", lineNum, err)
		}
	}

	// Ensure default user exists
	if _, ok := m.users["default"]; !ok {
		m.users["default"] = &shared.ACLUser{
			Name:        "default",
			Enabled:     true,
			NoPass:      true,
			AllCommands: true,
			AllKeys:     true,
			AllChannels: true,
			Categories:  make(map[shared.CommandCategory]bool),
			Commands:    make(map[shared.CommandType]bool),
			Subcommands: make(map[shared.CommandType]map[string]bool),
		}
	}

	m.aclFile = path
	return scanner.Err()
}

// SaveToFile saves ACL rules to a file
func (m *ACLManager) SaveToFile(path string) error {
	token := m.mu.RLock()
	defer m.mu.RUnlock(token)

	if path == "" {
		path = m.aclFile
	}
	if path == "" {
		return errors.New("ERR no ACL file configured")
	}

	// Write to temp file first
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, "acl-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := f.Name()

	// Sort users by name for deterministic output
	usernames := make([]string, 0, len(m.users))
	for name := range m.users {
		usernames = append(usernames, name)
	}
	sort.Strings(usernames)

	for _, username := range usernames {
		user := m.users[username]
		line := m.formatUserRule(user)
		if _, err := fmt.Fprintln(f, line); err != nil {
			closeErr := f.Close()
			removeErr := os.Remove(tmpPath)
			return errors.Join(err, closeErr, removeErr)
		}
	}

	if err := f.Close(); err != nil {
		removeErr := os.Remove(tmpPath)
		return errors.Join(err, removeErr)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, path); err != nil {
		removeErr := os.Remove(tmpPath)
		return errors.Join(err, removeErr)
	}

	return nil
}

// parseACLLine parses a single ACL rule line
func (m *ACLManager) parseACLLine(line string) error {
	fields := strings.Fields(line)
	if len(fields) < 2 || strings.ToLower(fields[0]) != "user" {
		return fmt.Errorf("invalid ACL line: must start with 'user'")
	}

	username := fields[1]
	user, ok := m.users[username]
	if !ok {
		user = &shared.ACLUser{
			Name:        username,
			Categories:  make(map[shared.CommandCategory]bool),
			Commands:    make(map[shared.CommandType]bool),
			Subcommands: make(map[shared.CommandType]map[string]bool),
		}
		m.users[username] = user
	}

	return m.applyACLRules(user, fields[2:])
}

// applyACLRules applies ACL rule tokens to a user
func (m *ACLManager) applyACLRules(user *shared.ACLUser, rules []string) error {
	for _, rule := range rules {
		if err := m.applyACLRule(user, rule); err != nil {
			return err
		}
	}
	return nil
}

// applyACLRule applies a single ACL rule to a user
func (m *ACLManager) applyACLRule(user *shared.ACLUser, rule string) error {
	rule = strings.ToLower(rule)

	switch {
	case rule == "on":
		user.Enabled = true
	case rule == "off":
		user.Enabled = false
	case rule == "nopass":
		user.NoPass = true
	case rule == "resetpass":
		user.PasswordHashes = nil
		user.NoPass = false
	case rule == "allkeys" || rule == "~*":
		user.AllKeys = true
	case rule == "resetkeys":
		user.AllKeys = false
		user.KeyPatterns = nil
	case rule == "allchannels" || rule == "&*":
		user.AllChannels = true
	case rule == "resetchannels":
		user.AllChannels = false
		user.ChannelPatterns = nil
	case rule == "allcommands" || rule == "+@all":
		user.AllCommands = true
	case rule == "nocommands" || rule == "-@all":
		user.AllCommands = false
		user.Categories = make(map[shared.CommandCategory]bool)
		user.Commands = make(map[shared.CommandType]bool)
	case rule == "reset":
		// Reset to minimal state
		user.Enabled = false
		user.NoPass = false
		user.PasswordHashes = nil
		user.AllCommands = false
		user.AllKeys = false
		user.AllChannels = false
		user.Categories = make(map[shared.CommandCategory]bool)
		user.Commands = make(map[shared.CommandType]bool)
		user.KeyPatterns = nil
		user.ChannelPatterns = nil
		user.Selectors = nil

	case strings.HasPrefix(rule, ">"):
		// Plaintext password - hash it
		password := rule[1:]
		hash, err := HashPassword(password)
		if err != nil {
			return fmt.Errorf("failed to hash password: %w", err)
		}
		user.PasswordHashes = append(user.PasswordHashes, hash)
		user.NoPass = false

	case strings.HasPrefix(rule, "#"):
		// Pre-hashed password (bcrypt format)
		hashStr := rule[1:]
		user.PasswordHashes = append(user.PasswordHashes, []byte(hashStr))
		user.NoPass = false

	case strings.HasPrefix(rule, "<"):
		// Remove password
		password := rule[1:]
		newHashes := user.PasswordHashes[:0]
		for _, hash := range user.PasswordHashes {
			if bcrypt.CompareHashAndPassword(hash, []byte(password)) != nil {
				newHashes = append(newHashes, hash)
			}
		}
		user.PasswordHashes = newHashes

	case strings.HasPrefix(rule, "!"):
		// Remove hashed password
		hashStr := rule[1:]
		newHashes := user.PasswordHashes[:0]
		for _, hash := range user.PasswordHashes {
			if string(hash) != hashStr {
				newHashes = append(newHashes, hash)
			}
		}
		user.PasswordHashes = newHashes

	case strings.HasPrefix(rule, "+@"):
		// Add category
		catName := rule[2:]
		cat, ok := shared.GetCategoryByName(catName)
		if !ok {
			return fmt.Errorf("unknown category: %s", catName)
		}
		if cat == shared.CatAll {
			user.AllCommands = true
		} else {
			user.Categories[cat] = true
		}

	case strings.HasPrefix(rule, "-@"):
		// Remove category
		catName := rule[2:]
		cat, ok := shared.GetCategoryByName(catName)
		if !ok {
			return fmt.Errorf("unknown category: %s", catName)
		}
		if cat == shared.CatAll {
			user.AllCommands = false
		} else {
			user.Categories[cat] = false
		}

	case strings.HasPrefix(rule, "+"):
		// Add command (may include |subcommand)
		cmdStr := rule[1:]
		if idx := strings.Index(cmdStr, "|"); idx > 0 {
			// Subcommand permission
			cmdName := cmdStr[:idx]
			subName := cmdStr[idx+1:]
			cmd, ok := shared.LookupCommandName(strings.ToUpper(cmdName))
			if !ok {
				return fmt.Errorf("unknown command: %s", cmdName)
			}
			if user.Subcommands[cmd] == nil {
				user.Subcommands[cmd] = make(map[string]bool)
			}
			user.Subcommands[cmd][subName] = true
		} else {
			cmd, ok := shared.LookupCommandName(strings.ToUpper(cmdStr))
			if !ok {
				return fmt.Errorf("unknown command: %s", cmdStr)
			}
			user.Commands[cmd] = true
		}

	case strings.HasPrefix(rule, "-"):
		// Remove command
		cmdStr := rule[1:]
		if idx := strings.Index(cmdStr, "|"); idx > 0 {
			cmdName := cmdStr[:idx]
			subName := cmdStr[idx+1:]
			cmd, ok := shared.LookupCommandName(strings.ToUpper(cmdName))
			if !ok {
				return fmt.Errorf("unknown command: %s", cmdName)
			}
			if user.Subcommands[cmd] == nil {
				user.Subcommands[cmd] = make(map[string]bool)
			}
			user.Subcommands[cmd][subName] = false
		} else {
			cmd, ok := shared.LookupCommandName(strings.ToUpper(cmdStr))
			if !ok {
				return fmt.Errorf("unknown command: %s", cmdStr)
			}
			user.Commands[cmd] = false
		}

	case strings.HasPrefix(rule, "~"):
		// Key pattern (read/write)
		pattern := rule[1:]
		user.KeyPatterns = append(user.KeyPatterns, shared.KeyPattern{
			Pattern: pattern,
			Type:    shared.KeyPatternReadWrite,
		})

	case strings.HasPrefix(rule, "%r~"):
		// Key pattern (read only)
		pattern := rule[3:]
		user.KeyPatterns = append(user.KeyPatterns, shared.KeyPattern{
			Pattern: pattern,
			Type:    shared.KeyPatternReadOnly,
		})

	case strings.HasPrefix(rule, "%w~"):
		// Key pattern (write only)
		pattern := rule[3:]
		user.KeyPatterns = append(user.KeyPatterns, shared.KeyPattern{
			Pattern: pattern,
			Type:    shared.KeyPatternWriteOnly,
		})

	case strings.HasPrefix(rule, "&"):
		// Channel pattern
		pattern := rule[1:]
		if pattern == "*" {
			user.AllChannels = true
		} else {
			user.ChannelPatterns = append(user.ChannelPatterns, pattern)
		}

	default:
		return fmt.Errorf("unknown ACL rule: %s", rule)
	}

	return nil
}

// formatUserRule formats a user's ACL rules as a string
func (m *ACLManager) formatUserRule(user *shared.ACLUser) string {
	var parts []string
	parts = append(parts, "user", user.Name)

	if user.Enabled {
		parts = append(parts, "on")
	} else {
		parts = append(parts, "off")
	}

	if user.NoPass {
		parts = append(parts, "nopass")
	}

	// Output password hashes with # prefix
	for _, hash := range user.PasswordHashes {
		parts = append(parts, "#"+string(hash))
	}

	// Key patterns
	if user.AllKeys {
		parts = append(parts, "~*")
	} else {
		for _, kp := range user.KeyPatterns {
			switch kp.Type {
			case shared.KeyPatternReadWrite:
				parts = append(parts, "~"+kp.Pattern)
			case shared.KeyPatternReadOnly:
				parts = append(parts, "%R~"+kp.Pattern)
			case shared.KeyPatternWriteOnly:
				parts = append(parts, "%W~"+kp.Pattern)
			}
		}
	}

	// Channel patterns
	if user.AllChannels {
		parts = append(parts, "&*")
	} else {
		for _, cp := range user.ChannelPatterns {
			parts = append(parts, "&"+cp)
		}
	}

	// Command permissions
	if user.AllCommands {
		parts = append(parts, "+@all")
	} else {
		// Categories
		for cat, allowed := range user.Categories {
			name := shared.GetCategoryName(cat)
			if allowed {
				parts = append(parts, "+@"+name)
			} else {
				parts = append(parts, "-@"+name)
			}
		}
		// Individual commands
		for cmd, allowed := range user.Commands {
			name := cmd.String()
			if allowed {
				parts = append(parts, "+"+name)
			} else {
				parts = append(parts, "-"+name)
			}
		}
	}

	return strings.Join(parts, " ")
}

// aclMatchGlob performs simple glob matching (*, ?)
func aclMatchGlob(pattern, str string) bool {
	return aclMatchGlobRecursive(pattern, str, 0, 0)
}

func aclMatchGlobRecursive(pattern, str string, pi, si int) bool {
	for pi < len(pattern) {
		if si >= len(str) {
			// Remaining pattern must be all *
			for pi < len(pattern) {
				if pattern[pi] != '*' {
					return false
				}
				pi++
			}
			return true
		}

		switch pattern[pi] {
		case '*':
			// Try matching 0 or more characters
			for i := si; i <= len(str); i++ {
				if aclMatchGlobRecursive(pattern, str, pi+1, i) {
					return true
				}
			}
			return false
		case '?':
			// Match exactly one character
			pi++
			si++
		case '[':
			// Character class
			pi++
			negated := false
			if pi < len(pattern) && pattern[pi] == '^' {
				negated = true
				pi++
			}
			matched := false
			for pi < len(pattern) && pattern[pi] != ']' {
				if pi+2 < len(pattern) && pattern[pi+1] == '-' {
					// Range
					if str[si] >= pattern[pi] && str[si] <= pattern[pi+2] {
						matched = true
					}
					pi += 3
				} else {
					if pattern[pi] == str[si] {
						matched = true
					}
					pi++
				}
			}
			if pi < len(pattern) {
				pi++ // skip ']'
			}
			if matched == negated {
				return false
			}
			si++
		default:
			if pattern[pi] != str[si] {
				return false
			}
			pi++
			si++
		}
	}

	return si == len(str)
}
