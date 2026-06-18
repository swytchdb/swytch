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

package cluster

import (
	"testing"

	"github.com/quic-go/quic-go"
)

// TestDispatchPlaintextEmptyPayload guards the remote-input path: a zstd
// frame can decompress to zero bytes, which must not panic the dispatcher.
func TestDispatchPlaintextEmptyPayload(t *testing.T) {
	transport := NewQUICNotifyTransport(1, nil, func(NodeId) *quic.Conn { return nil }, nil)

	transport.dispatchPlaintext(2, nil)
	transport.dispatchPlaintext(2, []byte{})
}
