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

package shared

import "strings"

// KeyPatternType indicates what operations are permitted by a key pattern
type KeyPatternType int

const (
	KeyPatternReadWrite KeyPatternType = iota // ~pattern (read + write)
	KeyPatternReadOnly                        // %R~pattern (read only)
	KeyPatternWriteOnly                       // %W~pattern (write only)
)

// KeyPattern represents a key access pattern
type KeyPattern struct {
	Pattern string
	Type    KeyPatternType
}

// ACLUser represents a Redis ACL user
type ACLUser struct {
	Name           string
	Enabled        bool
	PasswordHashes [][]byte // bcrypt hashes (includes salt)
	NoPass         bool     // allow without password

	// Command permissions
	AllCommands bool                            // +@all
	Categories  map[CommandCategory]bool        // +@read, -@write, etc.
	Commands    map[CommandType]bool            // +get, -set, etc.
	Subcommands map[CommandType]map[string]bool // +config|get, etc.

	// Key permissions
	AllKeys     bool         // ~*
	KeyPatterns []KeyPattern // ~pattern, %R~pattern, %W~pattern

	// Channel permissions (for pub/sub)
	AllChannels     bool     // &*
	ChannelPatterns []string // &pattern

	// Selectors (additional permission sets, Redis 7.0+)
	Selectors []*ACLSelector
}

// ACLSelector represents an additional permission set that can be applied conditionally
type ACLSelector struct {
	Commands    map[CommandType]bool
	Categories  map[CommandCategory]bool
	AllCommands bool
	KeyPatterns []KeyPattern
	AllKeys     bool
}

// GetUserForDisplay returns user info suitable for ACL GETUSER
func (u *ACLUser) GetUserForDisplay() map[string]any {
	info := make(map[string]any)
	info["flags"] = u.getFlags()

	passwords := make([]string, len(u.PasswordHashes))
	for i, hash := range u.PasswordHashes {
		passwords[i] = string(hash)
	}

	info["passwords"] = passwords

	if u.AllCommands {
		info["commands"] = "+@all"
	} else {
		var cmds []string

		for cat, allowed := range u.Categories {
			name := GetCategoryName(cat)
			if allowed {
				cmds = append(cmds, "+@"+name)
			} else {
				cmds = append(cmds, "-@"+name)
			}
		}

		for cmd, allowed := range u.Commands {
			name := cmd.String()
			if allowed {
				cmds = append(cmds, "+"+name)
			} else {
				cmds = append(cmds, "-"+name)
			}
		}

		info["commands"] = strings.Join(cmds, " ")
	}

	if u.AllKeys {
		info["keys"] = []string{"~*"}
	} else {
		var keys []string

		for _, kp := range u.KeyPatterns {
			switch kp.Type {
			case KeyPatternReadWrite:
				keys = append(keys, "~"+kp.Pattern)
			case KeyPatternReadOnly:
				keys = append(keys, "%R~"+kp.Pattern)
			case KeyPatternWriteOnly:
				keys = append(keys, "%W~"+kp.Pattern)
			}
		}

		info["keys"] = keys
	}

	if u.AllChannels {
		info["channels"] = []string{"&*"}
	} else {
		chans := make([]string, 0, len(u.ChannelPatterns))
		for _, cp := range u.ChannelPatterns {
			chans = append(chans, "&"+cp)
		}

		info["channels"] = chans
	}

	return info
}

func (u *ACLUser) getFlags() []string {
	var flags []string
	if u.Enabled {
		flags = append(flags, "on")
	} else {
		flags = append(flags, "off")
	}

	if u.AllKeys {
		flags = append(flags, "allkeys")
	}

	if u.AllChannels {
		flags = append(flags, "allchannels")
	}

	if u.AllCommands {
		flags = append(flags, "allcommands")
	}

	if u.NoPass {
		flags = append(flags, "nopass")
	} else if len(u.PasswordHashes) == 0 {
		flags = append(flags, "nopass")
	}

	return flags
}
