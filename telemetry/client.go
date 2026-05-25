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
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"time"
)

const defaultEndpoint = "https://stats.getswytch.com"

type HeartbeatStats struct {
	Nodes         int
	UptimeSeconds int
	MemoryAvail   int64
	MemoryUsage   int64
	HitCount      uint64
	MissCount     uint64
	Evictions     uint64
	EntryCount    int
	Capacity      int
	AverageK      float64
}

type Config struct {
	NodeID       string
	ClusterID    string
	Version      string
	Features     string
	AdopterEmail string
	StatsFunc    func() HeartbeatStats
}

type Client struct {
	nodeID    string
	clusterID string
	version   string
	goos      string
	goarch    string
	features  string

	adopterEmail string
	statsFunc    func() HeartbeatStats

	httpClient        *http.Client
	endpoint          string
	heartbeatInterval time.Duration
}

func New(cfg Config) *Client {
	sf := cfg.StatsFunc
	if sf == nil {
		sf = func() HeartbeatStats { return HeartbeatStats{} }
	}
	return &Client{
		nodeID:            cfg.NodeID,
		clusterID:         cfg.ClusterID,
		version:           cfg.Version,
		goos:              runtime.GOOS,
		goarch:            runtime.GOARCH,
		features:          cfg.Features,
		adopterEmail:      cfg.AdopterEmail,
		statsFunc:         sf,
		httpClient:        &http.Client{Timeout: 5 * time.Second},
		endpoint:          defaultEndpoint,
		heartbeatInterval: 5 * time.Minute,
	}
}

func (c *Client) Run(ctx context.Context) {
	c.sendStartup(ctx)
	if c.adopterEmail != "" {
		c.sendAdopter(ctx)
	}

	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			finalCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			c.sendHeartbeat(finalCtx)
			cancel()
			return
		case <-ticker.C:
			c.sendHeartbeat(ctx)
		}
	}
}

func (c *Client) sendStartup(ctx context.Context) {
	params := url.Values{}
	params.Set("ev", "startup")
	params.Set("node_id", c.nodeID)
	if c.clusterID != "" {
		params.Set("cluster_id", c.clusterID)
	}
	params.Set("ver", c.version)
	params.Set("os", c.goos)
	params.Set("arch", c.goarch)
	if c.features != "" {
		params.Set("features", c.features)
	}
	c.doGet(ctx, params)
}

func (c *Client) sendHeartbeat(ctx context.Context) {
	stats := c.statsFunc()
	params := url.Values{}
	params.Set("ev", "heartbeat")
	params.Set("node_id", c.nodeID)
	if c.clusterID != "" {
		params.Set("cluster_id", c.clusterID)
	}
	params.Set("ver", c.version)
	params.Set("nodes", strconv.Itoa(stats.Nodes))
	params.Set("uptime", strconv.Itoa(stats.UptimeSeconds))
	if stats.MemoryAvail != 0 {
		params.Set("memory_available", strconv.FormatInt(stats.MemoryAvail, 10))
	}
	if stats.MemoryUsage != 0 {
		params.Set("memory_usage", strconv.FormatInt(stats.MemoryUsage, 10))
	}
	if stats.HitCount != 0 {
		params.Set("hit_count", strconv.FormatUint(stats.HitCount, 10))
	}
	if stats.MissCount != 0 {
		params.Set("miss_count", strconv.FormatUint(stats.MissCount, 10))
	}
	if stats.Evictions != 0 {
		params.Set("evictions", strconv.FormatUint(stats.Evictions, 10))
	}
	if stats.EntryCount != 0 {
		params.Set("entry_count", strconv.Itoa(stats.EntryCount))
	}
	if stats.Capacity != 0 {
		params.Set("capacity", strconv.Itoa(stats.Capacity))
	}
	if stats.AverageK != 0 {
		params.Set("average_k", strconv.FormatFloat(stats.AverageK, 'f', 2, 64))
	}
	c.doGet(ctx, params)
}

func (c *Client) sendAdopter(ctx context.Context) {
	params := url.Values{}
	params.Set("ev", "early-adopter")
	params.Set("node_id", c.nodeID)
	params.Set("email", base64.StdEncoding.EncodeToString([]byte(c.adopterEmail)))
	c.doGet(ctx, params)
}

func (c *Client) doGet(ctx context.Context, params url.Values) {
	u := c.endpoint + "/t/v1/nearcache?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		slog.Debug("telemetry: build request", "error", err)
		return
	}
	resp, err := c.httpClient.Do(req)
	if resp != nil && resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if err != nil {
		slog.Debug("telemetry: send", "error", err)
	}
}
