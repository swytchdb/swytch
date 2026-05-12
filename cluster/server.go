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
	"fmt"
	"io"
	"log/slog"

	"github.com/quic-go/quic-go"
	pb "github.com/swytchdb/swytch/cluster/proto"
)

// handleStream dispatches an incoming QUIC bidirectional stream based on its type prefix.
func handleStream(stream *quic.Stream, logReader LogReader, forwardHandler ForwardHandler) {
	defer func() {
		if err := stream.Close(); err != nil {
			slog.Debug("failed to close QUIC stream", "error", err)
		}
	}()

	// Read 1-byte stream type
	var typeBuf [1]byte
	if _, err := io.ReadFull(stream, typeBuf[:]); err != nil {
		slog.Debug("failed to read stream type", "error", err)
		return
	}

	switch typeBuf[0] {
	case streamTypeFetch:
		handleFetchStream(stream, logReader)
	case streamTypeForward:
		handleForwardStream(stream, forwardHandler)
	default:
		slog.Warn("unknown stream type", "type", typeBuf[0])
	}
}

// handleFetchStream reads a FetchRequest, serves the effect data, and writes
// the FetchResponse back on the same stream.
func handleFetchStream(stream *quic.Stream, logReader LogReader) {
	req := &pb.FetchRequest{}
	if err := readMessage(stream, req); err != nil {
		slog.Debug("failed to read fetch request", "error", err)
		return
	}

	if logReader == nil {
		resp := &pb.FetchResponse{}
		if err := writeMessage(stream, resp); err != nil {
			slog.Debug("failed to write empty fetch response", "error", err)
		}
		return
	}

	data, err := logReader.ReadEffect(req.GetRef())
	if err != nil {
		slog.Debug("effect not found for fetch",
			"offset", req.GetRef(),
			"error", err,
		)
		// Write empty response to signal not found
		resp := &pb.FetchResponse{
			Ref: req.GetRef(),
		}
		if err := writeMessage(stream, resp); err != nil {
			slog.Debug("failed to write not-found fetch response", "error", err)
		}
		return
	}

	RecordFetchServed()

	resp := &pb.FetchResponse{
		Ref:        req.GetRef(),
		EffectData: data,
	}
	if err := writeMessage(stream, resp); err != nil {
		slog.Debug("failed to write fetch response",
			"offset", req.GetRef(),
			"error", err,
		)
	}
}

// handleForwardStream reads a ForwardedTransaction, executes it via the
// ForwardHandler, and writes the ForwardedResponse back on the stream.
func handleForwardStream(stream *quic.Stream, handler ForwardHandler) {
	if handler == nil {
		slog.Warn("received forward stream but no ForwardHandler configured")
		resp := &pb.ForwardedResponse{Error: true, ErrorMessage: "forwarding not supported"}
		_ = writeMessage(stream, resp)
		return
	}

	req := &pb.ForwardedTransaction{}
	if err := readMessage(stream, req); err != nil {
		slog.Debug("failed to read forwarded transaction", "error", err)
		return
	}

	resp := handler.HandleForwardedTransaction(req)
	if err := writeMessage(stream, resp); err != nil {
		slog.Debug("failed to write forwarded response", "error", err)
	}
}

// acceptStreams accepts incoming QUIC bidirectional streams on a connection and dispatches them.
func acceptStreams(conn *quic.Conn, logReader LogReader, forwardHandler ForwardHandler) {
	for {
		stream, err := conn.AcceptStream(conn.Context())
		if err != nil {
			return // connection closed
		}
		go handleStream(stream, logReader, forwardHandler)
	}
}

// errFetchNotFound is returned when a fetch response contains no effect data.
var errFetchNotFound = fmt.Errorf("effect not found on peer")
