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
	"github.com/swytchdb/swytch/redis/shared"
)

// testUser is a default user with all permissions for testing
var testUser = &shared.ACLUser{
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
