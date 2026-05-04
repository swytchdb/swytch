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

package telemetry

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"sync"
	"testing"
	"time"
)

type recorder struct {
	mu       sync.Mutex
	requests []url.Values
}

func (r *recorder) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.requests = append(r.requests, req.URL.Query())
		r.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
}

func (r *recorder) get() []url.Values {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]url.Values, len(r.requests))
	copy(cp, r.requests)
	return cp
}

func newTestClient(ts *httptest.Server, cfg Config) *Client {
	c := New(cfg)
	c.endpoint = ts.URL
	c.heartbeatInterval = 50 * time.Millisecond
	return c
}

func dummyStats() HeartbeatStats {
	return HeartbeatStats{Nodes: 3, UptimeSeconds: 120}
}

func TestStartupEvent(t *testing.T) {
	rec := &recorder{}
	ts := httptest.NewServer(rec.handler())
	defer ts.Close()

	c := newTestClient(ts, Config{
		NodeID:    "abc123",
		Version:   "1.0.0",
		Features:  "redis",
		StatsFunc: dummyStats,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()

	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	reqs := rec.get()
	if len(reqs) == 0 {
		t.Fatal("expected at least one request")
	}

	startup := reqs[0]
	if startup.Get("ev") != "startup" {
		t.Errorf("expected ev=startup, got %q", startup.Get("ev"))
	}
	if startup.Get("node_id") != "abc123" {
		t.Errorf("expected node_id=abc123, got %q", startup.Get("node_id"))
	}
	if startup.Get("ver") != "1.0.0" {
		t.Errorf("expected ver=1.0.0, got %q", startup.Get("ver"))
	}
	if startup.Get("os") != runtime.GOOS {
		t.Errorf("expected os=%s, got %q", runtime.GOOS, startup.Get("os"))
	}
	if startup.Get("arch") != runtime.GOARCH {
		t.Errorf("expected arch=%s, got %q", runtime.GOARCH, startup.Get("arch"))
	}
	if startup.Get("features") != "redis" {
		t.Errorf("expected features=redis, got %q", startup.Get("features"))
	}
}

func TestHeartbeatEvent(t *testing.T) {
	rec := &recorder{}
	ts := httptest.NewServer(rec.handler())
	defer ts.Close()

	c := newTestClient(ts, Config{
		NodeID:  "def456",
		Version: "2.0.0",
		StatsFunc: func() HeartbeatStats {
			return HeartbeatStats{
				Nodes:         5,
				UptimeSeconds: 300,
				MemoryAvail:   1024,
				MemoryUsage:   512,
				HitCount:      100,
				MissCount:     10,
				Evictions:     3,
				EntryCount:    50,
				Capacity:      200,
				AverageK:      1.5,
			}
		},
	})

	ctx := context.Background()
	c.sendHeartbeat(ctx)

	reqs := rec.get()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 request, got %d", len(reqs))
	}

	hb := reqs[0]
	if hb.Get("ev") != "heartbeat" {
		t.Errorf("expected ev=heartbeat, got %q", hb.Get("ev"))
	}
	if hb.Get("node_id") != "def456" {
		t.Errorf("expected node_id=def456, got %q", hb.Get("node_id"))
	}
	if hb.Get("nodes") != "5" {
		t.Errorf("expected nodes=5, got %q", hb.Get("nodes"))
	}
	if hb.Get("uptime") != "300" {
		t.Errorf("expected uptime=300, got %q", hb.Get("uptime"))
	}
	if hb.Get("memory_available") != "1024" {
		t.Errorf("expected memory_available=1024, got %q", hb.Get("memory_available"))
	}
	if hb.Get("memory_usage") != "512" {
		t.Errorf("expected memory_usage=512, got %q", hb.Get("memory_usage"))
	}
	if hb.Get("hit_count") != "100" {
		t.Errorf("expected hit_count=100, got %q", hb.Get("hit_count"))
	}
	if hb.Get("miss_count") != "10" {
		t.Errorf("expected miss_count=10, got %q", hb.Get("miss_count"))
	}
	if hb.Get("evictions") != "3" {
		t.Errorf("expected evictions=3, got %q", hb.Get("evictions"))
	}
	if hb.Get("entry_count") != "50" {
		t.Errorf("expected entry_count=50, got %q", hb.Get("entry_count"))
	}
	if hb.Get("capacity") != "200" {
		t.Errorf("expected capacity=200, got %q", hb.Get("capacity"))
	}
	if hb.Get("average_k") != "1.50" {
		t.Errorf("expected average_k=1.50, got %q", hb.Get("average_k"))
	}
}

func TestHeartbeatOmitsZeroOptionals(t *testing.T) {
	rec := &recorder{}
	ts := httptest.NewServer(rec.handler())
	defer ts.Close()

	c := newTestClient(ts, Config{
		NodeID:  "aaa",
		Version: "1.0.0",
		StatsFunc: func() HeartbeatStats {
			return HeartbeatStats{Nodes: 1, UptimeSeconds: 60}
		},
	})

	c.sendHeartbeat(context.Background())

	reqs := rec.get()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 request, got %d", len(reqs))
	}
	hb := reqs[0]
	for _, key := range []string{"memory_available", "memory_usage", "hit_count", "miss_count", "evictions", "entry_count", "capacity", "average_k"} {
		if hb.Get(key) != "" {
			t.Errorf("expected %s omitted, got %q", key, hb.Get(key))
		}
	}
}

func TestAdopterEvent(t *testing.T) {
	rec := &recorder{}
	ts := httptest.NewServer(rec.handler())
	defer ts.Close()

	c := newTestClient(ts, Config{
		NodeID:       "bbb",
		Version:      "1.0.0",
		AdopterEmail: "test@example.com",
		StatsFunc:    dummyStats,
	})

	c.sendAdopter(context.Background())

	reqs := rec.get()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 request, got %d", len(reqs))
	}
	ev := reqs[0]
	if ev.Get("ev") != "early-adopter" {
		t.Errorf("expected ev=early-adopter, got %q", ev.Get("ev"))
	}
	if ev.Get("node_id") != "bbb" {
		t.Errorf("expected node_id=bbb, got %q", ev.Get("node_id"))
	}
	wantEmail := base64.StdEncoding.EncodeToString([]byte("test@example.com"))
	if ev.Get("email") != wantEmail {
		t.Errorf("expected email=%s, got %q", wantEmail, ev.Get("email"))
	}
}

func TestNoRequests(t *testing.T) {
	rec := &recorder{}
	ts := httptest.NewServer(rec.handler())
	defer ts.Close()

	_ = newTestClient(ts, Config{
		NodeID:  "ccc",
		Version: "1.0.0",
		StatsFunc: func() HeartbeatStats {
			return HeartbeatStats{Nodes: 1, UptimeSeconds: 0}
		},
	})

	time.Sleep(20 * time.Millisecond)

	reqs := rec.get()
	if len(reqs) != 0 {
		t.Errorf("expected 0 requests without Run, got %d", len(reqs))
	}
}

func TestInviteMeOverride(t *testing.T) {
	rec := &recorder{}
	ts := httptest.NewServer(rec.handler())
	defer ts.Close()

	c := newTestClient(ts, Config{
		NodeID:       "ddd",
		Version:      "1.0.0",
		AdopterEmail: "early@adopter.com",
		StatsFunc:    dummyStats,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()

	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	reqs := rec.get()
	hasStartup := false
	hasAdopter := false
	for _, r := range reqs {
		switch r.Get("ev") {
		case "startup":
			hasStartup = true
		case "early-adopter":
			hasAdopter = true
			wantEmail := base64.StdEncoding.EncodeToString([]byte("early@adopter.com"))
			if r.Get("email") != wantEmail {
				t.Errorf("expected email=%s, got %q", wantEmail, r.Get("email"))
			}
		}
	}
	if !hasStartup {
		t.Error("expected startup event")
	}
	if !hasAdopter {
		t.Error("expected early-adopter event")
	}
}

func TestCancelStopsTicker(t *testing.T) {
	rec := &recorder{}
	ts := httptest.NewServer(rec.handler())
	defer ts.Close()

	c := newTestClient(ts, Config{
		NodeID:  "eee",
		Version: "1.0.0",
		StatsFunc: func() HeartbeatStats {
			return HeartbeatStats{Nodes: 1, UptimeSeconds: 1}
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Run did not stop within 100ms of ctx cancel")
	}
}

func TestHTTPErrorsSwallowed(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	c := newTestClient(ts, Config{
		NodeID:  "fff",
		Version: "1.0.0",
		StatsFunc: func() HeartbeatStats {
			return HeartbeatStats{Nodes: 1, UptimeSeconds: 1}
		},
	})

	c.sendStartup(context.Background())
	c.sendHeartbeat(context.Background())
	c.sendAdopter(context.Background())
}

func TestFinalHeartbeatOnShutdown(t *testing.T) {
	rec := &recorder{}
	ts := httptest.NewServer(rec.handler())
	defer ts.Close()

	c := newTestClient(ts, Config{
		NodeID:  "ggg",
		Version: "1.0.0",
		StatsFunc: func() HeartbeatStats {
			return HeartbeatStats{Nodes: 2, UptimeSeconds: 42}
		},
	})
	c.heartbeatInterval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()

	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	reqs := rec.get()
	hasHeartbeat := false
	for _, r := range reqs {
		if r.Get("ev") == "heartbeat" {
			hasHeartbeat = true
			if r.Get("nodes") != "2" {
				t.Errorf("expected nodes=2, got %q", r.Get("nodes"))
			}
			if r.Get("uptime") != "42" {
				t.Errorf("expected uptime=42, got %q", r.Get("uptime"))
			}
		}
	}
	if !hasHeartbeat {
		t.Error("expected a final heartbeat on shutdown")
	}
}

func TestNilStatsFuncDoesNotPanic(t *testing.T) {
	rec := &recorder{}
	ts := httptest.NewServer(rec.handler())
	defer ts.Close()

	c := newTestClient(ts, Config{
		NodeID:  "hhh",
		Version: "1.0.0",
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.Run(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Run did not stop within 100ms of ctx cancel")
	}

	reqs := rec.get()
	hasStartup := false
	hasHeartbeat := false
	for _, r := range reqs {
		switch r.Get("ev") {
		case "startup":
			hasStartup = true
		case "heartbeat":
			hasHeartbeat = true
		}
	}
	if !hasStartup {
		t.Error("expected startup event even with nil StatsFunc")
	}
	if !hasHeartbeat {
		t.Error("expected final heartbeat even with nil StatsFunc")
	}
}
