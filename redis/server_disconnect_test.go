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
	"io"
	"net"
	"runtime"
	"testing"
	"time"
)

func TestHandleConnection_SubscribeDisconnect_NoPanic(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldProcs)

	s, err := NewServer(DefaultServerConfig())
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	defer func() {
		s.cancel()
		s.handler.Close()
	}()

	subscribeCmd := "*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n"

	for i := range 256 {
		serverConn, clientConn := net.Pipe()

		done := make(chan struct{})
		// Pre-fill semaphore slot (normally done by acceptLoop)
		if s.connSem != nil {
			s.connSem <- struct{}{}
		}

		s.wg.Add(1)
		go func() {
			s.handleConnection(serverConn)
			close(done)
		}()

		if _, err := io.WriteString(clientConn, subscribeCmd); err != nil {
			t.Fatalf("WriteString() iteration %d error = %v", i, err)
		}

		_ = clientConn.Close()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("handleConnection did not exit after disconnect (iteration %d)", i)
		}
	}
}
