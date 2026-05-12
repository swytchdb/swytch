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
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"time"

	"github.com/quic-go/quic-go"
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/tracing"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/proto"
)

// QUICNotifyTransport replaces the custom UDP transport (Noise + chunking + pacing)
// with QUIC stream-per-message. Each notification opens a unidirectional stream,
// writes the plaintext, and closes. QUIC handles encryption (TLS 1.3),
// retransmission, flow control, and congestion control.
type QUICNotifyTransport struct {
	nodeID  NodeId
	handler PacketHandler

	// connFunc returns the QUIC connection for a given peer.
	// Set by PeerManager to look up PeerConn connections.
	connFunc func(peerID NodeId) *quic.Conn

	// onPeerIdentified, when non-nil, is invoked the first time we
	// receive a uni-stream on an accepted connection — the 8-byte
	// sender header tells us the peer's real NodeID, and the owning
	// PeerManager uses it to register the connection as an inbound
	// return path for ACK/NACK traffic. Nil is legal (used by tests
	// that don't exercise the accept side).
	onPeerIdentified func(peerID NodeId, conn *quic.Conn)

	stopCh chan struct{}
}

// NewQUICNotifyTransport creates a new QUIC notification transport.
// onPeerIdentified may be nil when the caller doesn't need inbound
// connection tracking (e.g. unit tests).
func NewQUICNotifyTransport(
	nodeID NodeId,
	handler PacketHandler,
	connFunc func(peerID NodeId) *quic.Conn,
	onPeerIdentified func(peerID NodeId, conn *quic.Conn),
) *QUICNotifyTransport {
	return &QUICNotifyTransport{
		nodeID:           nodeID,
		handler:          handler,
		connFunc:         connFunc,
		onPeerIdentified: onPeerIdentified,
		stopCh:           make(chan struct{}),
	}
}

// Start is a no-op for the QUIC transport (connections are managed by PeerConn).
func (t *QUICNotifyTransport) Start() {}

// Stop shuts down the transport.
func (t *QUICNotifyTransport) Stop() {
	close(t.stopCh)
}

// Send opens a unidirectional QUIC stream, writes [2:senderNodeID LE][plaintext],
// and closes the stream. QUIC handles encryption, retransmission, and flow control.
// Returns the total bytes written and any error.
func (t *QUICNotifyTransport) Send(peerID NodeId, plaintext []byte) (wireSize int, err error) {
	conn := t.connFunc(peerID)
	if conn == nil {
		return 0, ErrPeerUnavailable
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := conn.OpenUniStreamSync(ctx)
	if err != nil {
		return 0, err
	}

	// Write sender nodeID prefix so the receiver can identify us
	var header [8]byte
	binary.LittleEndian.PutUint64(header[:], uint64(t.nodeID))
	if _, err := stream.Write(header[:]); err != nil {
		stream.CancelWrite(1)
		return 0, err
	}

	if _, err := stream.Write(plaintext); err != nil {
		stream.CancelWrite(1)
		return 0, err
	}

	stream.Close() // sends FIN
	RecordQUICStreamOpened()
	return len(plaintext) + 8, nil
}

// SendDirect sends a control packet (heartbeat, ACK) via a QUIC uni-stream.
// With QUIC, there is no distinction between paced and direct sends — each
// message gets its own independent stream.
func (t *QUICNotifyTransport) SendDirect(peerID NodeId, plaintext []byte) error {
	_, err := t.Send(peerID, plaintext)
	return err
}

// AcceptUniStreams accepts incoming unidirectional streams on a QUIC connection
// and dispatches them. Called from the accept loop for each accepted connection.
// Goroutine lifetime is tied to the connection, not the transport — it exits
// when the connection closes (conn.Context() cancelled).
func (t *QUICNotifyTransport) AcceptUniStreams(conn *quic.Conn) {
	for {
		stream, err := conn.AcceptUniStream(conn.Context())
		if err != nil {
			return // connection closed
		}
		go t.handleInboundUniStream(conn, stream)
	}
}

// handleInboundUniStream reads a complete message from a uni-stream and dispatches it.
// Wire format: [8:senderNodeID LE][plaintext message]
func (t *QUICNotifyTransport) handleInboundUniStream(conn *quic.Conn, stream *quic.ReceiveStream) {
	_, readSpan := tracing.Tracer().Start(context.Background(), "transport.read_stream")

	// Read sender nodeID prefix
	var header [8]byte
	if _, err := io.ReadFull(stream, header[:]); err != nil {
		slog.Debug("failed to read uni-stream sender header", "error", err)
		RecordQUICStreamError()
		readSpan.End()
		return
	}
	peerID := NodeId(binary.LittleEndian.Uint64(header[:]))

	// Now that we know the peer's real NodeID, register the inbound
	// connection so outbound ACK/NACK traffic addressed to peerID can
	// route back through it. The callback is a no-op for conns we've
	// already registered.
	if t.onPeerIdentified != nil {
		t.onPeerIdentified(peerID, conn)
	}

	// Read the plaintext message (limited to maxFrameSize to prevent abuse)
	data, err := io.ReadAll(io.LimitReader(stream, maxFrameSize))
	if err != nil || len(data) < 1 {
		if err != nil {
			slog.Debug("failed to read uni-stream data", "peer", peerID, "error", err)
			RecordQUICStreamError()
		}
		readSpan.End()
		return
	}

	readSpan.SetAttributes(
		attribute.Int("peer.id", int(peerID)),
		attribute.Int("packet.type", int(data[0])),
		attribute.Int("packet.size", len(data)),
	)
	readSpan.End()

	t.dispatchPlaintext(peerID, data)
}

// dispatchPlaintext routes decrypted plaintext to the appropriate handler.
// Same as UDPTransport.dispatchPlaintext but without chunking support.
func (t *QUICNotifyTransport) dispatchPlaintext(peerID NodeId, plaintext []byte) {
	switch plaintext[0] {
	case PacketTypeHeartbeat:
		hbPeerID, timestamp, err := ParseHeartbeat(plaintext)
		if err != nil {
			return
		}
		if hbPeerID != peerID {
			slog.Warn("heartbeat payload nodeID mismatch", "payload", hbPeerID, "sender", peerID)
			return
		}
		if t.handler != nil {
			t.handler.HandleHeartbeat(peerID, timestamp)
		}

	case PacketTypeHeartbeatACK:
		hbPeerID, timestamp, err := ParseHeartbeat(plaintext)
		if err != nil {
			return
		}
		if hbPeerID != peerID {
			slog.Warn("heartbeat ACK payload nodeID mismatch", "payload", hbPeerID, "sender", peerID)
			return
		}
		if t.handler != nil {
			t.handler.HandleHeartbeatACK(peerID, timestamp)
		}

	case PacketTypeNotify:
		requestID, notify, err := parseNotifyPacket(plaintext)
		if err != nil {
			slog.Debug("invalid notify packet", "peer", peerID, "error", err)
			return
		}
		if t.handler != nil {
			go t.handler.HandleNotify(peerID, requestID, notify)
		}

	case PacketTypeNotifyACK:
		ackPeerID, requestID, status, credits, err := parseNotifyACKPacket(plaintext)
		if err != nil {
			slog.Debug("invalid notify ACK packet", "peer", peerID, "error", err)
			return
		}
		if ackPeerID != peerID {
			slog.Warn("notify ACK payload nodeID mismatch", "payload", ackPeerID, "sender", peerID)
			return
		}
		if t.handler != nil {
			if status == 0x01 {
				t.handler.HandleNotifyACKWithData(peerID, requestID, status, credits, plaintext)
			} else {
				t.handler.HandleNotifyACK(peerID, requestID, status, credits)
			}
		}

	case PacketTypeNack:
		nack := &pb.NackNotify{}
		if err := proto.Unmarshal(plaintext[1:], nack); err != nil {
			slog.Debug("invalid nack packet", "peer", peerID, "error", err)
			return
		}
		if t.handler != nil {
			t.handler.HandleNackNotify(peerID, nack)
		}

	default:
		slog.Debug("unknown packet type on QUIC uni-stream", "type", plaintext[0], "peer", peerID)
	}
}
