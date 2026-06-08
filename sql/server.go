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

// Package sql exposes a SQLite-dialect SQL surface over the PostgreSQL
// wire protocol. It is the scaffolding for a swytch-backed SQL layer; in
// this phase, SQLite runs in :memory: per connection with no swytch
// storage wired in.
package sql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"

	wire "github.com/jeroenrinzema/psql-wire"
	"zombiezen.com/go/sqlite"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
)

// ServerConfig holds configuration for the SQL server.
type ServerConfig struct {
	// Address is the TCP address to listen on (e.g. ":5433").
	Address string

	// AdvertisedVersion is the string sent as server_version during the
	// pg wire startup handshake. Defaults to "17.0 (Swytch)".
	AdvertisedVersion string

	// Logger is an optional *slog.Logger. When nil, slog.Default() is used.
	Logger *slog.Logger

	// Engine is the swytch effects engine used as the storage layer.
	// When nil, NewServer constructs a standalone single-node engine.
	// Shared with other swytch subsystems (cluster, beacon) when
	// provided externally.
	Engine *effects.Engine

	// ownsEngine marks whether the Server should Close the engine on
	// Stop — true when NewServer constructed the engine itself.
	ownsEngine bool
}

// DefaultServerConfig returns a default server configuration.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Address:           ":5433",
		AdvertisedVersion: "17.0 (Swytch)",
	}
}

// Server is a PostgreSQL-wire-compatible SQL server that executes SQLite
// dialect SQL. Each client connection is assigned its own SQLite
// connection opened in ":memory:" mode; swytch is the authoritative
// storage for all tables created through this server.
type Server struct {
	cfg ServerConfig
	log *slog.Logger

	wire   *wire.Server
	engine *effects.Engine

	// parserConn is a dedicated in-memory SQLite connection used to
	// parse DDL statements (via pragma_table_info) before rewriting
	// them as CREATE VIRTUAL TABLE on session connections. Single
	// goroutine access via parserMu.
	parserConn *sqlite.Conn
	parserMu   sync.Mutex

	// module is the lazily-built swytch vtab module registered on
	// every session SQLite conn. A shared instance is fine because
	// the module's callbacks close over *Server, not a specific conn.
	module *sqlite.Module

	// nextID supplies unique session ids for logging.
	nextID atomic.Uint64

	// rowIDSeq feeds the synthetic-rowid generator as the per-insert
	// distinguisher passed to deriveID. Incrementing it plus the
	// (HLC, NodeID) inputs guarantees unique hash inputs per allocation.
	rowIDSeq atomic.Uint64

	// schemaEpoch bumps on every DDL that mutates a table's shape
	// (CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX, ALTER
	// TABLE). Sessions track their last-seen epoch; at query time,
	// if session.schemaEpoch < server.schemaEpoch, the session
	// redeclares any vtabs whose column lists drifted. Steady-state
	// is zero-cost: everyone's epoch matches, check is a single
	// atomic load.
	schemaEpoch atomic.Uint64

	// sessionsByConn maps a session's SQLite connection back to the
	// session struct. Populated in initSession; consulted by the
	// vtab's Module.Connect callback so vtable instances can hold a
	// session reference for access to the session's active
	// transactional effects.Context. Plain map + RWMutex (not
	// sync.Map) — low-churn and type-safe.
	sessionsByConn struct {
		mu sync.RWMutex
		m  map[*sqlite.Conn]*session
	}

	listener net.Listener
	doneCh   chan error
}

// NewServer builds a Server with the given configuration. It does not
// begin listening — call Start or StartAsync.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.AdvertisedVersion == "" {
		cfg.AdvertisedVersion = "17.0 (Swytch)"
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	// Stand up a single-node effects engine if the caller didn't
	// supply one. This is what lets `swytch sql` run standalone
	// (useful in tests and during development).
	engine := cfg.Engine
	ownsEngine := false
	if engine == nil {
		engine = effects.NewEngine(effects.EngineConfig{
			NodeID:      pb.NewNodeID(),
			DefaultMode: effects.SafeMode,
		})
		ownsEngine = true
	}

	parserConn, err := sqlite.OpenConn(":memory:")
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("sql: open parser conn: %w", err),
			closeOwnedEngineOnError(engine, ownsEngine),
		)
	}

	s := &Server{
		cfg:        cfg,
		log:        log,
		engine:     engine,
		parserConn: parserConn,
	}
	s.cfg.ownsEngine = ownsEngine
	s.sessionsByConn.m = make(map[*sqlite.Conn]*session)
	s.module = s.newVTabModule()

	ws, err := wire.NewServer(
		s.handle,
		wire.SessionMiddleware(s.initSession),
		wire.CloseConn(s.closeSession),
		wire.GlobalParameters(wire.Parameters{
			// Required by pgx's simple query protocol — without it
			// pgx refuses to run queries because it can't reason
			// about backslash escaping in string literals.
			"standard_conforming_strings": "on",
			// Custom, for dialect-aware ORMs.
			dialectParamKey: dialectValue,
		}),
	)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("sql: wire.NewServer: %w", err),
			closeParserOnError(parserConn),
			closeOwnedEngineOnError(engine, ownsEngine),
		)
	}
	ws.Version = cfg.AdvertisedVersion
	s.wire = ws

	return s, nil
}

// closeParserOnError and closeOwnedEngineOnError wrap cleanup on the
// error path of NewServer so that downstream close failures propagate
// via errors.Join rather than being silently dropped.

func closeParserOnError(conn *sqlite.Conn) error {
	if err := conn.Close(); err != nil {
		return fmt.Errorf("sql: close parser conn: %w", err)
	}
	return nil
}

func closeOwnedEngineOnError(engine *effects.Engine, owns bool) error {
	if !owns {
		return nil
	}
	if err := engine.Close(); err != nil {
		return fmt.Errorf("sql: close engine: %w", err)
	}
	return nil
}

// Engine returns the underlying effects engine.
func (s *Server) Engine() *effects.Engine {
	return s.engine
}

// Start listens on the configured address and serves until the listener
// is closed. It blocks.
func (s *Server) Start() error {
	listener, err := s.listen()
	if err != nil {
		return err
	}
	return s.serve(listener)
}

// StartAsync listens on the configured address and serves in a
// background goroutine. The bound address is available via Addr once
// this method returns.
func (s *Server) StartAsync() error {
	listener, err := s.listen()
	if err != nil {
		return err
	}
	s.doneCh = make(chan error, 1)
	go func() {
		s.doneCh <- s.serve(listener)
	}()
	return nil
}

// Addr returns the network address the server is listening on, or nil
// if it has not started yet.
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Stop shuts the server down, waiting for in-flight queries to
// complete or the context to expire. Errors from each step are
// collected via errors.Join so one failing close doesn't hide others.
func (s *Server) Stop(ctx context.Context) error {
	if s.wire == nil {
		return nil
	}
	var errs []error
	if err := s.wire.Shutdown(ctx); err != nil && !errors.Is(err, net.ErrClosed) {
		errs = append(errs, fmt.Errorf("sql: wire shutdown: %w", err))
	}
	if s.listener != nil {
		if err := s.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, fmt.Errorf("sql: close listener: %w", err))
		}
	}
	// psql-wire's Shutdown above drains active connections and calls
	// our CloseConn hook for each, which invokes session.Close. We
	// don't need to iterate anything ourselves.
	if s.parserConn != nil {
		if err := s.parserConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("sql: close parser conn: %w", err))
		}
		s.parserConn = nil
	}
	if s.cfg.ownsEngine && s.engine != nil {
		if err := s.engine.Close(); err != nil {
			errs = append(errs, fmt.Errorf("sql: close engine: %w", err))
		}
		s.engine = nil
	}
	return errors.Join(errs...)
}

// Done returns a channel that receives the error (if any) from the
// serve loop when StartAsync was used. Nil when Start was used.
func (s *Server) Done() <-chan error {
	return s.doneCh
}

func (s *Server) listen() (net.Listener, error) {
	listener, err := net.Listen("tcp", s.cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("sql: listen %s: %w", s.cfg.Address, err)
	}
	s.listener = listener
	s.log.Info("sql server listening",
		"address", listener.Addr(),
		"version", s.cfg.AdvertisedVersion,
	)
	return listener, nil
}

func (s *Server) serve(listener net.Listener) error {
	err := s.wire.Serve(listener)
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}
