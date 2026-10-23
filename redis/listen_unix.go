//go:build linux || darwin || freebsd || netbsd || openbsd

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
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// reusePortListenConfig returns a ListenConfig with SO_REUSEPORT enabled.
// This allows multiple listeners to bind to the same port for better
// load distribution across CPU cores.
func reusePortListenConfig() net.ListenConfig {
	return net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error

			err := c.Control(func(fd uintptr) {
				opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
			})
			if err != nil {
				return err
			}

			return opErr
		},
	}
}

// supportsReusePort returns true on Unix platforms that support SO_REUSEPORT.
func supportsReusePort() bool {
	return true
}
