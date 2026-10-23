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

import (
	"sync"
	"sync/atomic"
)

const (
	// PubsubMsgChanSize is the buffer size for the pub/sub message channel
	PubsubMsgChanSize = 1000
)

// PubSubMessage represents a message to be delivered to subscribers
type PubSubMessage struct {
	Type    string // "message", "pmessage", "subscribe", "unsubscribe", "psubscribe", "punsubscribe"
	Pattern string // for pmessage only
	Channel string
	Payload []byte
	Count   int // subscription count for sub/unsub responses
}

// PubSubClient represents a client in pub/sub mode
type PubSubClient struct {
	conn            *Connection
	msgChan         chan *PubSubMessage
	done            chan struct{}
	protocol        ProtocolVersion
	droppedMessages atomic.Int64
	mu              sync.Mutex
	closed          bool
	busy            bool // true while executing a command (skip async delivery)
}

// NewPubSubClient creates a new pub/sub client
func NewPubSubClient(conn *Connection, protocol ProtocolVersion) *PubSubClient {
	return &PubSubClient{
		conn:     conn,
		msgChan:  make(chan *PubSubMessage, PubsubMsgChanSize),
		done:     make(chan struct{}),
		protocol: protocol,
	}
}

// Send sends a message to the client's message channel (non-blocking)
func (c *PubSubClient) Send(msg *PubSubMessage) bool {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()

		return false
	}
	c.mu.Unlock()

	select {
	case c.msgChan <- msg:
		return true
	default:
		// Channel full, drop message
		c.droppedMessages.Add(1)

		return false
	}
}

// SetBusy marks the client as busy (during command execution)
func (c *PubSubClient) SetBusy(busy bool) {
	c.mu.Lock()
	c.busy = busy
	c.mu.Unlock()
}

// IsBusy returns true if the client is currently executing a command
func (c *PubSubClient) IsBusy() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.busy
}

// Close closes the client's message channel
func (c *PubSubClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.closed {
		c.closed = true
		close(c.done)
	}
}

// MsgChan returns the message channel for reading
func (c *PubSubClient) MsgChan() <-chan *PubSubMessage {
	return c.msgChan
}

// Done returns a channel that is closed when the client is closed
func (c *PubSubClient) Done() <-chan struct{} {
	return c.done
}
