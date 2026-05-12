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
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/swytchdb/swytch/redis/pubsub"
	"github.com/swytchdb/swytch/redis/shared"
	"github.com/swytchdb/swytch/tracing"
)

// ServerConfig holds configuration for the Redis server
type ServerConfig struct {
	// Address to listen on (e.g., ":6379")
	Address string

	// UnixSocket is the path to a Unix domain socket
	UnixSocket string

	// UnixSocketMode is the file mode for the Unix socket
	UnixSocketMode os.FileMode

	// NumDatabases is the number of databases (default 16)
	NumDatabases int

	// CapacityPerDB is the cache capacity per database
	CapacityPerDB int

	// MemoryLimit is the maximum memory in bytes (0 = unlimited)
	MemoryLimit int64

	// Password for AUTH command (empty = no auth required)
	// Deprecated: use ACLFile instead
	Password string

	// ACLFile is the path to the ACL file for user authentication
	ACLFile string

	// ReadTimeout is the timeout for reading commands
	ReadTimeout time.Duration

	// WriteTimeout is the timeout for writing responses
	WriteTimeout time.Duration

	// MaxConnections limits concurrent connections (0 = unlimited)
	MaxConnections int

	// DebugLogging enables debug-level command logging
	DebugLogging bool

	// TCPKeepAlive enables TCP keepalive
	TCPKeepAlive time.Duration

	// ReadBufferSize is the size of the read buffer per connection
	ReadBufferSize int

	// WriteBufferSize is the size of the write buffer per connection
	WriteBufferSize int

	// NumAcceptors is the number of parallel accept loops (0 = auto, uses NumCPU on supported platforms)
	// On Linux/macOS/BSD, uses SO_REUSEPORT to allow multiple listeners on the same port.
	// On Windows, this is ignored (single acceptor only).
	NumAcceptors int

	// TLSCertFile is the path to the TLS certificate file (PEM format)
	TLSCertFile string

	// TLSKeyFile is the path to the TLS private key file (PEM format)
	TLSKeyFile string

	// TLSCAFile is the path to the CA certificate for client verification (mTLS)
	TLSCAFile string

	// TLSMinVersion is the minimum TLS version (e.g., "1.2", "1.3")
	TLSMinVersion string
}

// DefaultServerConfig returns a default server configuration
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Address:       ":6379",
		NumDatabases:  shared.DefaultNumDatabases,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024, // 64MB
	}
}

// TLSEnabled returns true if TLS is configured
func (c *ServerConfig) TLSEnabled() bool {
	return c.TLSCertFile != ""
}

// parseTLSVersion converts a version string to the corresponding tls constant
func parseTLSVersion(version string) (uint16, error) {
	switch version {
	case "1.2", "":
		return tls.VersionTLS12, nil
	case "1.3":
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("invalid TLS version: %s (valid: 1.2, 1.3)", version)
	}
}

// buildTLSConfig creates a tls.Config from the server configuration.
// Returns nil, nil if TLS is not configured.
func (c *ServerConfig) buildTLSConfig() (*tls.Config, error) {
	if c.TLSCertFile == "" {
		return nil, nil
	}

	// Load server certificate and key
	cert, err := tls.LoadX509KeyPair(c.TLSCertFile, c.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
	}

	// Parse minimum TLS version
	minVersion, err := parseTLSVersion(c.TLSMinVersion)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   minVersion,
	}

	// Configure client certificate verification (mTLS) if CA file is specified
	if c.TLSCAFile != "" {
		caCert, err := os.ReadFile(c.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}

		tlsConfig.ClientCAs = caPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsConfig, nil
}

// Server is a Redis-compatible TCP server
type Server struct {
	config    ServerConfig
	handler   shared.CommandHandler
	listeners []net.Listener
	stats     *shared.Stats

	// Connection limiting
	connSem chan struct{}

	// Client tracking for CLIENT LIST
	clients *ClientRegistry

	// Active connection tracking for graceful shutdown
	connsMu sync.Mutex
	conns   map[net.Conn]struct{}

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewServer creates a new Redis server
// Returns an error if persistent storage is configured but fails to initialize.
func NewServer(config ServerConfig) (*Server, error) {
	var handler shared.CommandHandler

	{
		handlerCfg := HandlerConfig{
			NumDatabases:  config.NumDatabases,
			CapacityPerDB: config.CapacityPerDB,
			MemoryLimit:   config.MemoryLimit,
			Password:      config.Password,
			ACLFile:       config.ACLFile,
			RequireAuth:   config.Password != "" || config.ACLFile != "",
			DebugLogging:  config.DebugLogging,
		}
		handler = NewHandler(handlerCfg)
		slog.Info("storage backend initialized", "type", "CloxCache")
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create stats
	stats := shared.NewStats()

	// Create client registry
	clients := NewClientRegistry()

	// Set stats and client registry on handler if it supports it
	if h, ok := handler.(*Handler); ok {
		h.SetStats(stats)
		h.SetClientRegistry(clients)
	}

	s := &Server{
		config:  config,
		handler: handler,
		stats:   stats,
		clients: clients,
		conns:   make(map[net.Conn]struct{}),
		ctx:     ctx,
		cancel:  cancel,
	}

	if config.MaxConnections > 0 {
		s.connSem = make(chan struct{}, config.MaxConnections)
	}

	return s, nil
}

// Start begins listening for connections
func (s *Server) Start() error {
	// Build TLS config (nil if not configured)
	tlsConfig, err := s.config.buildTLSConfig()
	if err != nil {
		return fmt.Errorf("TLS configuration error: %w", err)
	}

	if s.config.UnixSocket != "" {
		// Unix sockets don't benefit from SO_REUSEPORT, use single listener
		lc := net.ListenConfig{}

		// Remove existing socket file
		if err := os.Remove(s.config.UnixSocket); err != nil && !os.IsNotExist(err) {
			slog.Warn("failed to remove existing socket", "path", s.config.UnixSocket, "error", err)
		}

		listener, err := lc.Listen(context.Background(), "unix", s.config.UnixSocket)
		if err != nil {
			return err
		}

		// Wrap with TLS if configured
		if tlsConfig != nil {
			listener = tls.NewListener(listener, tlsConfig)
		}
		s.listeners = []net.Listener{listener}

		if s.config.UnixSocketMode != 0 {
			if err := os.Chmod(s.config.UnixSocket, s.config.UnixSocketMode); err != nil {
				_ = listener.Close() // Best effort cleanup
				return err
			}
		}

		s.wg.Add(1)
		go s.acceptLoop(listener)

		if tlsConfig != nil {
			slog.Info("redis server listening (TLS)", "socket", s.config.UnixSocket)
		} else {
			slog.Info("redis server listening", "socket", s.config.UnixSocket)
		}
	} else if s.config.Address != "" {
		// Determine number of acceptors
		numAcceptors := s.config.NumAcceptors
		if numAcceptors <= 0 {
			numAcceptors = runtime.NumCPU()
		}

		// On platforms without SO_REUSEPORT, fall back to single acceptor
		if !supportsReusePort() {
			numAcceptors = 1
		}

		// Use SO_REUSEPORT on supported platforms
		lc := reusePortListenConfig()

		for i := 0; i < numAcceptors; i++ {
			listener, err := lc.Listen(context.Background(), "tcp", s.config.Address)
			if err != nil {
				// Clean up already-created listeners
				for _, l := range s.listeners {
					_ = l.Close()
				}
				s.listeners = nil
				return err
			}

			// Wrap with TLS if configured
			if tlsConfig != nil {
				listener = tls.NewListener(listener, tlsConfig)
			}
			s.listeners = append(s.listeners, listener)

			s.wg.Add(1)
			go s.acceptLoop(listener)
		}

		if tlsConfig != nil {
			slog.Info("redis server listening (TLS)", "address", s.config.Address, "acceptors", numAcceptors)
		} else {
			slog.Info("redis server listening", "address", s.config.Address, "acceptors", numAcceptors)
		}
	}

	return nil
}

// ListenAndServe starts the server and blocks until it's stopped
func (s *Server) ListenAndServe() error {
	if err := s.Start(); err != nil {
		return err
	}
	<-s.ctx.Done()
	return nil
}

// Stop gracefully stops the server
func (s *Server) Stop() error {
	s.cancel()

	var errs []error

	// Close all listeners first (stops accepting new connections)
	for _, listener := range s.listeners {
		if err := listener.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing listener: %w", err))
		}
	}

	// Snapshot connections under the lock, then close outside to avoid
	// contention with connection handler cleanup that also acquires connsMu.
	s.connsMu.Lock()
	snapshot := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		snapshot = append(snapshot, conn)
	}
	s.connsMu.Unlock()

	for _, conn := range snapshot {
		if err := conn.Close(); err != nil {
			slog.Debug("failed to close client connection during shutdown", "error", err)
		}
	}

	// Wait for all handlers to finish with a timeout
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All handlers finished cleanly
	case <-time.After(5 * time.Second):
		slog.Warn("server shutdown timed out waiting for connections to close")
	}

	if s.config.UnixSocket != "" {
		if err := os.Remove(s.config.UnixSocket); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("removing unix socket: %w", err))
		}
	}

	s.handler.Close()

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Addr returns the server's listening address
func (s *Server) Addr() net.Addr {
	if len(s.listeners) == 0 {
		return nil
	}
	return s.listeners[0].Addr()
}

// Stats returns the server statistics
func (s *Server) Stats() *shared.Stats {
	return s.stats
}

// Handler returns the command handler
func (s *Server) Handler() shared.CommandHandler {
	return s.handler
}

func (s *Server) acceptLoop(listener net.Listener) {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		if s.connSem != nil {
			select {
			case s.connSem <- struct{}{}:
			case <-s.ctx.Done():
				return
			}
		}

		conn, err := listener.Accept()
		if err != nil {
			if s.connSem != nil {
				<-s.connSem
			}

			select {
			case <-s.ctx.Done():
				return
			default:
				slog.Error("accept error", "error", err)
				continue
			}
		}

		// Configure TCP connection for high performance
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if s.config.TCPKeepAlive > 0 {
				_ = tcpConn.SetKeepAlive(true)
				_ = tcpConn.SetKeepAlivePeriod(s.config.TCPKeepAlive)
			}
			// Set OS-level socket buffers to reduce syscalls
			// 256KB buffers allow more data to be batched before syscalls
			_ = tcpConn.SetReadBuffer(256 * 1024)
			_ = tcpConn.SetWriteBuffer(256 * 1024)
			// Disable Nagle's algorithm for lower latency
			_ = tcpConn.SetNoDelay(true)
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// connState holds per-connection state
type connState struct {
	reader *bufio.Reader
	writer *bufio.Writer
	parser *shared.Parser
	conn   *shared.Connection
}

var connStatePool = sync.Pool{
	New: func() any {
		return &connState{}
	},
}

func (s *Server) getConnState(conn net.Conn, ctx context.Context) *connState {
	cs := connStatePool.Get().(*connState)

	readBufSz := s.config.ReadBufferSize
	if readBufSz <= 0 {
		readBufSz = shared.ReadBufSize
	}
	writeBufSz := s.config.WriteBufferSize
	if writeBufSz <= 0 {
		writeBufSz = shared.DefaultBufSize
	}

	cs.reader = bufio.NewReaderSize(conn, readBufSz)
	cs.writer = bufio.NewWriterSize(conn, writeBufSz)
	cs.parser = shared.NewParserWithReader(cs.reader)
	cs.conn = &shared.Connection{
		SelectedDB: 0,
		User:       nil, // Will be set on first command if default user has nopass
		RemoteAddr: conn.RemoteAddr().String(),
		Ctx:        ctx,
		Stats:      s.stats,
	}

	// Create per-connection effects context if engine is available
	if h, ok := s.handler.(*Handler); ok && h.engine != nil {
		cs.conn.EffectsCtx = h.engine.NewContext()
	}

	return cs
}

func putConnState(cs *connState) {
	cs.reader = nil
	cs.writer = nil
	cs.parser = nil
	cs.conn = nil
	connStatePool.Put(cs)
}

// readResult carries a command or error from the reader goroutine
type readResult struct {
	cmd          *shared.Command
	protocolErr  string // non-empty means send this error to client and continue
	quit         bool   // QUIT command received
	moreBuffered bool   // true if more data is buffered (for pipelining)
}

const commandQueueDepth = 8

func (s *Server) handleConnection(conn net.Conn) {
	s.stats.ConnectionOpened()

	// Track connection for graceful shutdown
	s.connsMu.Lock()
	s.conns[conn] = struct{}{}
	s.connsMu.Unlock()

	// Create a context for this connection, derived from server context
	connCtx, connCancel := context.WithCancel(s.ctx)

	defer s.wg.Done()
	defer func() {
		connCancel() // Cancel context when connection closes
		_ = conn.Close()
		s.stats.ConnectionClosed()
		if s.connSem != nil {
			<-s.connSem
		}
		// Remove from tracked connections
		s.connsMu.Lock()
		delete(s.conns, conn)
		s.connsMu.Unlock()
	}()

	// Track pubsub for cleanup at connection close
	var pubsubCleanupFunc func()

	cs := s.getConnState(conn, connCtx)

	// Channel for commands from reader goroutine.
	// A small queue lets the reader stay ahead on pipelined connections.
	cmdChan := make(chan readResult, commandQueueDepth)

	// Signal when reader goroutine exits (must wait before putConnState)
	readerDone := make(chan struct{})

	// Cleanup - wait for reader before returning connState to pool.
	// Drain any unread queued commands so pooled buffers are returned.
	// Cancel context and close connection first to unblock the reader goroutine
	// if it's blocked in ReadCommandInto (prevents deadlock on early executor exit).
	defer func() {
		connCancel()
		_ = conn.Close()
		<-readerDone // Wait for reader goroutine to exit
		for result := range cmdChan {
			if result.cmd != nil {
				shared.PutCommand(result.cmd)
			}
		}
		putConnState(cs)
	}()

	// Cleanup pubsub subscriptions on disconnect
	defer func() {
		if pubsubCleanupFunc != nil {
			pubsubCleanupFunc()
		}
	}()

	// Register client for CLIENT LIST tracking
	s.clients.Register(cs.conn)
	defer s.clients.Unregister(cs.conn)

	// Get writer from pool
	w := shared.GetWriter()
	w.SetStats(s.stats)
	defer shared.PutWriter(w)

	// Mutex for writing to the connection (needed for pub/sub async writes)
	var connWriteMu sync.Mutex

	// Track whether pub/sub delivery goroutine has been started
	var pubsubDeliveryStarted bool

	// Check if this is a TLS connection for pre-handshake timeout
	_, isTLS := conn.(*tls.Conn)

	// Reader goroutine - reads commands and cancels context on disconnect
	go func() {
		defer close(readerDone) // Signal that reader has exited
		defer connCancel()      // Cancel context when reader exits (EOF/disconnect)
		defer close(cmdChan)

		firstRead := true

		// For TLS connections, set aggressive timeout for handshake
		// Non-TLS traffic (HTTP/SSH bots) will timeout quickly instead of hanging
		if isTLS {
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		} else if s.config.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
		}

		for {
			// Get a fresh command from pool for each read to avoid races
			cmd := shared.GetCommand()

			parsedCmd, err := cs.parser.ReadCommandInto(cmd)
			if err != nil {
				shared.PutCommand(cmd) // Return unused command to pool

				// Check if server is shutting down
				select {
				case <-s.ctx.Done():
					return
				default:
				}

				// EOF or closed connection - exit cleanly
				if errors.Is(err, io.EOF) || isClosedError(err) {
					return
				}

				// Timeout - reset deadline and continue (cheap check first)
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					if s.config.ReadTimeout > 0 {
						_ = conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
					}
					continue
				}

				// Line too long - fatal
				if errors.Is(err, shared.ErrLineTooLong) {
					return
				}

				// TLS handshake error - close without sending error response
				// Bots sending HTTP/SSH can't read Redis protocol anyway (expensive check last)
				if isTLSHandshakeError(err) {
					slog.Warn("TLS handshake failed", "remote", conn.RemoteAddr())
					return
				}

				// Protocol error - send to executor to respond.
				// Use a fixed message instead of err.Error() — net.OpError.Error()
				// is extremely expensive (stringifies addresses via reflection).
				select {
				case cmdChan <- readResult{protocolErr: "ERR protocol error"}:
				case <-connCtx.Done():
					return
				}
				continue
			}

			// First successful read - clear handshake timeout and apply normal timeout
			if firstRead {
				firstRead = false
				_ = conn.SetDeadline(time.Time{}) // Clear all deadlines
				if s.config.ReadTimeout > 0 {
					_ = conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
				}
			} else if s.config.ReadTimeout > 0 {
				// Reset read deadline on subsequent successful reads
				_ = conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
			}

			// Handle QUIT specially
			if parsedCmd.Type == shared.CmdQuit {
				shared.PutCommand(cmd)
				select {
				case cmdChan <- readResult{quit: true}:
				case <-connCtx.Done():
				}
				return
			}

			// Check if more data is buffered (for pipelining detection)
			// Do this in reader goroutine to avoid data race with main goroutine
			moreBuffered := cs.parser.Buffered() > 0

			// Send command to executor (executor will PutCommand when done)
			select {
			case cmdChan <- readResult{cmd: parsedCmd, moreBuffered: moreBuffered}:
			case <-connCtx.Done():
				shared.PutCommand(cmd)
				return
			}
		}
	}()

	pendingWrites := false

	// Main loop - executes commands from reader goroutine
	for result := range cmdChan {
		// Handle protocol errors
		if result.protocolErr != "" {
			// Flush pending writes first
			if pendingWrites {
				connWriteMu.Lock()
				if flushErr := cs.writer.Flush(); flushErr != nil {
					connWriteMu.Unlock()
					return
				}
				connWriteMu.Unlock()
				pendingWrites = false
			}
			w.Buffer().Reset()
			w.WriteError(result.protocolErr)
			connWriteMu.Lock()
			_, _ = cs.writer.Write(w.Buffer().Bytes())
			_ = cs.writer.Flush()
			connWriteMu.Unlock()
			continue
		}

		// Handle QUIT
		if result.quit {
			if pendingWrites {
				connWriteMu.Lock()
				_ = cs.writer.Flush()
				connWriteMu.Unlock()
			}
			return
		}

		cmd := result.cmd

		// Start a lifecycle span that covers the full command processing
		lifecycleCtx, lifecycleSpan := tracing.Tracer().Start(cs.conn.Ctx, "server.command_lifecycle")
		cs.conn.Ctx = lifecycleCtx

		// Track last command for CLIENT LIST
		cmdName := cmd.Type.String()
		if cmdName != "" {
			s.clients.SetLastCmd(cs.conn, cmdName)
		}

		// Flush pending writes before blocking commands
		// This ensures pipelined responses (e.g., LPUSH response before BLPOP)
		// are sent before we block waiting for data
		if pendingWrites && isBlockingCommand(cmd.Type) {
			if s.config.WriteTimeout > 0 {
				_ = conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
			}
			connWriteMu.Lock()
			if flushErr := cs.writer.Flush(); flushErr != nil {
				connWriteMu.Unlock()
				shared.PutCommand(cmd)
				lifecycleSpan.End()
				return
			}
			connWriteMu.Unlock()
			pendingWrites = false
		}

		// Mark client as busy during command execution (for pub/sub ordering)
		if cs.conn.PubSubClient != nil {
			cs.conn.PubSubClient.SetBusy(true)
		}

		// Execute command and write response while holding mutex
		// This ensures pub/sub messages are delivered AFTER the command response
		connWriteMu.Lock()
		w.Buffer().Reset()
		w.SetProtocol(cs.conn.Protocol)
		s.handler.ExecuteInto(cmd, w, cs.conn)

		// Return command to pool after execution
		shared.PutCommand(cmd)

		// Write response
		if w.Buffer().Len() > 0 {
			_, writeErr := cs.writer.Write(w.Buffer().Bytes())
			if writeErr != nil {
				connWriteMu.Unlock()
				if cs.conn.PubSubClient != nil {
					cs.conn.PubSubClient.SetBusy(false)
				}
				return
			}
			pendingWrites = true
		}

		// For RESP3 clients, deliver pending pub/sub messages inline (after response)
		// This ensures proper ordering: command response first, then push messages
		if cs.conn.PubSubClient != nil && cs.conn.Protocol == shared.RESP3 {
			client := cs.conn.PubSubClient
			protocol := cs.conn.Protocol
			for {
				select {
				case msg := <-client.MsgChan():
					data := pubsub.FormatPubSubMessage(msg, protocol)
					if _, err := cs.writer.Write(data); err != nil {
						connWriteMu.Unlock()
						client.SetBusy(false)
						return
					}
					pendingWrites = true
				default:
					goto doneInlineDelivery
				}
			}
		doneInlineDelivery:
		}

		connWriteMu.Unlock()

		// Mark client as not busy
		if cs.conn.PubSubClient != nil {
			cs.conn.PubSubClient.SetBusy(false)
		}

		// Set up cleanup function and start delivery goroutine when entering pub/sub mode
		if cs.conn.PubSubClient != nil && !pubsubDeliveryStarted {
			pubsubDeliveryStarted = true
			pubsubCleanupFunc = func() {
				if cs.conn.PubSubClient != nil {
					if broker := shared.GetPubSubBroker(); broker != nil {
						broker.Cleanup(cs.conn.PubSubClient)
					}
					cs.conn.PubSubClient = nil
				}
			}

			// Start pub/sub message delivery goroutine for RESP2 clients only
			// RESP3 clients use inline delivery after each command for proper ordering
			if cs.conn.Protocol != shared.RESP3 {
				client := cs.conn.PubSubClient
				protocol := cs.conn.Protocol
				writer := cs.writer
				go func(client *shared.PubSubClient, protocol shared.ProtocolVersion, writer *bufio.Writer) {
					if client == nil || writer == nil {
						return
					}

					for {
						select {
						case <-connCtx.Done():
							return
						case <-client.Done():
							return
						case msg, ok := <-client.MsgChan():
							if !ok {
								return
							}
							// Format and write the message
							data := pubsub.FormatPubSubMessage(msg, protocol)
							connWriteMu.Lock()
							if s.config.WriteTimeout > 0 {
								_ = conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
							}
							_, writeErr := writer.Write(data)
							if writeErr == nil {
								_ = writer.Flush()
							}
							connWriteMu.Unlock()
							if writeErr != nil {
								return
							}
						}
					}
				}(client, protocol, writer)
			}
		}

		// Check if we exited pub/sub mode
		if cs.conn.PubSubClient == nil && pubsubDeliveryStarted {
			pubsubDeliveryStarted = false
			pubsubCleanupFunc = nil
		}

		// Flush when no more pipelined data
		if pendingWrites && !result.moreBuffered {
			_, flushSpan := tracing.Tracer().Start(lifecycleCtx, "server.flush_response")
			connWriteMu.Lock()
			if s.config.WriteTimeout > 0 {
				_ = conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
			}
			flushErr := cs.writer.Flush()
			connWriteMu.Unlock()
			flushSpan.End()
			if flushErr != nil {
				lifecycleSpan.End()
				return
			}
			pendingWrites = false
		}

		lifecycleSpan.End()
	}
}

func isClosedError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.ErrClosedPipe)
}

// isTLSHandshakeError returns true if the error is related to TLS handshake failure.
// These errors indicate the client sent non-TLS data (HTTP, SSH, etc.) to a TLS port,
// or there was a certificate issue. No point sending an error response.
//
// Uses string matching instead of errors.As to avoid reflection overhead —
// errors.As with interface targets walks the chain using reflect, which was
// consuming ~40% of total CPU in production.
func isTLSHandshakeError(err error) bool {
	// Fast type assertions first (no reflection)
	for e := err; e != nil; {
		switch e.(type) {
		case tls.RecordHeaderError:
			return true
		case *tls.AlertError:
			return true
		case x509.CertificateInvalidError:
			return true
		case x509.UnknownAuthorityError:
			return true
		}
		if uw, ok := e.(interface{ Unwrap() error }); ok {
			e = uw.Unwrap()
		} else {
			break
		}
	}
	return false
}

// isBlockingCommand returns true for commands that may block the connection
// waiting for data. These commands need pending writes flushed before execution.
func isBlockingCommand(cmd shared.CommandType) bool {
	switch cmd {
	case shared.CmdBLPop, shared.CmdBRPop, shared.CmdBLMPop, shared.CmdBLMove, shared.CmdBRPopLPush,
		shared.CmdBZPopMin, shared.CmdBZPopMax, shared.CmdBZMPop:
		return true
	default:
		return false
	}
}
