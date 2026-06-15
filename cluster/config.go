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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"github.com/quic-go/quic-go"
)

// NodeConfig represents a single node in the cluster.
type NodeConfig struct {
	ID      NodeId
	Address string // QUIC address (host:port)
	Region  string
}

// StorageConfig holds configuration for off-node object storage (Bunny CDN).
type StorageConfig struct {
	UploadEndpoint string // Bunny storage API (e.g. "https://storage.bunnycdn.com/zone")
	CDNEndpoint    string // Bunny CDN read (e.g. "https://cdn.example.com")
	AccessKey      string // Bunny storage access key
	BasePath       string // Node-specific prefix (derived from NodeID if empty)

	// HPKE keypair for post-quantum encryption (MLKEM768X25519 + AES-256-GCM).
	// Base64-encoded. Used as the default for key ranges without specific keys.
	HPKEPublicKey  string
	HPKEPrivateKey string
}

// ClusterConfig holds the full cluster topology.
type ClusterConfig struct {
	NodeID             NodeId        // This node's ID
	Nodes              []NodeConfig  // All nodes including self
	TLSPassphrase      string        // Shared passphrase for deterministic mTLS CA derivation
	ReplicationTimeout time.Duration // Max time to wait for replication ACK (default 5s)
	Storage            StorageConfig // Object storage for off-node durability
}

// Peers returns all nodes except self.
func (c *ClusterConfig) Peers() []NodeConfig {
	peers := make([]NodeConfig, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		if n.ID != c.NodeID {
			peers = append(peers, n)
		}
	}
	return peers
}

// Self returns this node's config, or nil if not found.
func (c *ClusterConfig) Self() *NodeConfig {
	for i := range c.Nodes {
		if c.Nodes[i].ID == c.NodeID {
			return &c.Nodes[i]
		}
	}
	return nil
}

// BuildTLSConfig returns a server-side TLS config for mTLS, derived from
// the shared passphrase. Returns nil, nil if TLSPassphrase is empty (insecure fallback).
func (c *ClusterConfig) BuildTLSConfig() (*tls.Config, error) {
	if c.TLSPassphrase == "" {
		return nil, nil
	}

	leaf, caPool, err := c.deriveTLS()
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{leaf},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// BuildClientTLSConfig returns a client-side TLS config for mTLS, derived from
// the shared passphrase. Returns nil, nil if TLSPassphrase is empty (insecure fallback).
func (c *ClusterConfig) BuildClientTLSConfig() (*tls.Config, error) {
	if c.TLSPassphrase == "" {
		return nil, nil
	}

	leaf, caPool, err := c.deriveTLS()
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{leaf},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// deriveTLS derives the CA from the passphrase and generates an ephemeral leaf
// certificate for this node. Returns the leaf tls.Certificate and a CA pool.
func (c *ClusterConfig) deriveTLS() (tls.Certificate, *x509.CertPool, error) {
	caKey, caCert, err := DeriveCAFromPassphrase(c.TLSPassphrase)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("failed to derive cluster CA: %w", err)
	}

	nodeAddr := ""
	if self := c.Self(); self != nil {
		nodeAddr = self.Address
	}

	leaf, err := GenerateLeafCert(caKey, caCert, nodeAddr)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("failed to generate cluster leaf cert: %w", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	return leaf, pool, nil
}

// clusterQUICConfig returns the shared QUIC config for all cluster connections.
// Tuned for high-throughput small-message notification traffic:
//   - Large connection-level flow control window (256 MB) so thousands of
//     concurrent uni-streams don't bottleneck on the shared connection window.
//   - Small per-stream window (64 KB) since each stream carries one small message.
//   - High stream limits (1M) to avoid stream ID exhaustion under load.
//   - AllowConnectionWindowIncrease always returns true to prevent QUIC from
//     throttling the receive window under sustained load.
func clusterQUICConfig() *quic.Config {
	return &quic.Config{
		MaxIncomingStreams:             1_000_000,
		MaxIncomingUniStreams:          1_000_000,
		KeepAlivePeriod:                10 * time.Second,
		InitialStreamReceiveWindow:     64 * 1024,         // 64 KB per stream (small messages)
		MaxStreamReceiveWindow:         1 * 1024 * 1024,   // 1 MB per stream max
		InitialConnectionReceiveWindow: 64 * 1024 * 1024,  // 64 MB connection window
		MaxConnectionReceiveWindow:     256 * 1024 * 1024, // 256 MB connection window max
		AllowConnectionWindowIncrease: func(conn *quic.Conn, delta uint64) bool {
			return true // always allow — LAN traffic, receiver is never the bottleneck
		},
	}
}

// TopologyDiff describes changes between two cluster configurations.
type TopologyDiff struct {
	Added   []NodeConfig // Peers in new but not old
	Removed []NodeConfig // Peers in old but not new
	Changed []NodeConfig // Peers whose address changed (contains the new config)
}

// DiffTopology computes the difference between two cluster configurations.
// Either old or new may be nil (treated as empty).
func DiffTopology(old, new *ClusterConfig) TopologyDiff {
	var diff TopologyDiff

	oldPeers := make(map[NodeId]NodeConfig)
	newPeers := make(map[NodeId]NodeConfig)

	if old != nil {
		for _, p := range old.Peers() {
			oldPeers[p.ID] = p
		}
	}
	if new != nil {
		for _, p := range new.Peers() {
			newPeers[p.ID] = p
		}
	}

	// Added: in new but not old
	for id, n := range newPeers {
		if _, exists := oldPeers[id]; !exists {
			diff.Added = append(diff.Added, n)
		}
	}

	// Removed: in old but not new
	for id, o := range oldPeers {
		if _, exists := newPeers[id]; !exists {
			diff.Removed = append(diff.Removed, o)
		}
	}

	// Changed: in both but address or region differs
	for id, n := range newPeers {
		if o, exists := oldPeers[id]; exists && (o.Address != n.Address || o.Region != n.Region) {
			diff.Changed = append(diff.Changed, n)
		}
	}

	return diff
}
