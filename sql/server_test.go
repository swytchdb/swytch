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

package sql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// startTestServer boots a Server on an ephemeral port and returns a
// database/sql handle pointed at it.
func startTestServer(t *testing.T) (*Server, *sql.DB) {
	t.Helper()

	logOut := io.Discard
	logLevel := slog.LevelInfo

	if testing.Verbose() {
		logOut = os.Stderr
		logLevel = slog.LevelDebug
	}

	srv, err := NewServer(ServerConfig{
		Address:           "127.0.0.1:0",
		AdvertisedVersion: "17.0",
		Logger:            slog.New(slog.NewTextHandler(logOut, &slog.HandlerOptions{Level: logLevel})),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	if err := srv.StartAsync(); err != nil {
		t.Fatalf("StartAsync: %v", err)
	}

	tcp, ok := srv.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("Server.Addr returned %T, want *net.TCPAddr", srv.Addr())
	}

	// Use simple query protocol so pgx doesn't issue Parse/Bind/Describe
	// messages that the phase-1 server doesn't handle yet.
	dsn := fmt.Sprintf(
		"host=127.0.0.1 port=%d user=test dbname=test sslmode=disable default_query_exec_mode=simple_protocol",
		tcp.Port,
	)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}

	db.SetMaxOpenConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var one int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			t.Logf("db.Close during setup: %v", closeErr)
		}

		shutdown(t, srv)
		t.Fatalf("probe SELECT 1: %v", err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("db.Close: %v", err)
		}

		shutdown(t, srv)
	})

	return srv, db
}

func shutdown(t *testing.T, srv *Server) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := srv.Stop(ctx); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

func TestSelectLiterals(t *testing.T) {
	_, db := startTestServer(t)

	t.Run("int", func(t *testing.T) {
		var got int64
		if err := db.QueryRow("SELECT 1").Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}

		if got != 1 {
			t.Errorf("got %d, want 1", got)
		}
	})

	t.Run("arith", func(t *testing.T) {
		var got int64
		if err := db.QueryRow("SELECT 1+1").Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}

		if got != 2 {
			t.Errorf("got %d, want 2", got)
		}
	})

	t.Run("text", func(t *testing.T) {
		var got string
		if err := db.QueryRow("SELECT 'hello'").Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}

		if got != "hello" {
			t.Errorf("got %q, want %q", got, "hello")
		}
	})

	t.Run("float", func(t *testing.T) {
		var got float64
		if err := db.QueryRow("SELECT 1.5").Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}

		if got != 1.5 {
			t.Errorf("got %v, want 1.5", got)
		}
	})

	t.Run("null", func(t *testing.T) {
		var got sql.NullString
		if err := db.QueryRow("SELECT NULL").Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}

		if got.Valid {
			t.Errorf("got %v, want NULL", got)
		}
	})
}

func TestMultipleRows(t *testing.T) {
	_, db := startTestServer(t)

	rows, err := db.Query("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("rows.Close: %v", err)
		}
	}()

	var got []int64

	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}

		got = append(got, v)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}

	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Errorf("got %v, want [1 2 3]", got)
	}
}

func TestCreateTableAcrossSessions(t *testing.T) {
	// In Phase 2, CREATE TABLE persists to swytch. A table created on
	// one pg connection must be visible to any subsequent pg
	// connection (via the table-list replay on session open).
	_, db := startTestServer(t)

	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(0)

	connA, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := connA.Close(); err != nil {
			t.Logf("connA.Close: %v", err)
		}
	}()

	if _, err := connA.ExecContext(context.Background(),
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Force connA's underlying pg conn closed so connB is a fresh
	// session that has to replay the table list.
	if err := connA.Close(); err != nil {
		t.Fatalf("close connA: %v", err)
	}

	connB, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := connB.Close(); err != nil {
			t.Logf("connB.Close: %v", err)
		}
	}()

	// Empty-scan should succeed (no rows, no error).
	rows, err := connB.QueryContext(context.Background(), "SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("select on second session: %v", err)
	}

	if rows.Next() {
		t.Error("expected empty result from fresh table")
	}

	if err := rows.Err(); err != nil {
		t.Errorf("rows.Err: %v", err)
	}

	if err := rows.Close(); err != nil {
		t.Errorf("rows.Close: %v", err)
	}
}

func TestCreateTableDuplicateName(t *testing.T) {
	_, db := startTestServer(t)

	if _, err := db.Exec("CREATE TABLE foo (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("first create: %v", err)
	}

	_, err := db.Exec("CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	if err == nil {
		t.Fatal("expected error creating duplicate table")
	}

	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("expected 'already exists' in error, got: %v", err)
	}
}

func TestShowSwytchDialect(t *testing.T) {
	_, db := startTestServer(t)

	var got string
	if err := db.QueryRow("SHOW swytch.dialect").Scan(&got); err != nil {
		t.Fatalf("SHOW: %v", err)
	}

	if got != "sqlite" {
		t.Errorf("SHOW swytch.dialect = %q, want %q", got, "sqlite")
	}
}

func TestSetSwytchDialectIsNoop(t *testing.T) {
	_, db := startTestServer(t)

	if _, err := db.Exec("SET swytch.dialect = 'anything'"); err != nil {
		t.Fatalf("SET: %v", err)
	}
}

func TestServerVersionParameter(t *testing.T) {
	// psql-wire v0.19.0 unconditionally sets server_version to a
	// hardcoded "150000" string during the handshake, overriding
	// srv.Version. Until that's fixed upstream (or we fork it via a
	// replace directive), the advertised version is not observable.
	t.Skip("blocked by psql-wire hardcoding server_version; see handshake.go:169")
}

func TestStartStop(t *testing.T) {
	srv, err := NewServer(ServerConfig{
		Address: "127.0.0.1:0",
		Logger:  slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	if err := srv.StartAsync(); err != nil {
		t.Fatalf("StartAsync: %v", err)
	}

	addr := srv.Addr()
	if addr == nil {
		t.Fatal("Addr nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := srv.Stop(ctx); err != nil {
		t.Errorf("Stop: %v", err)
	}

	// After Stop, Serve should have returned via Done.
	select {
	case err := <-srv.Done():
		if err != nil {
			t.Errorf("Done: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("server did not exit within 2s after Stop")
	}
}
