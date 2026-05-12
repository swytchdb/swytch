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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
)

// generateTestTLSConfig creates a self-signed TLS config for tests.
// Uses the same cert for both server and client (mTLS with InsecureSkipVerify).
func generateTestTLSConfig(t *testing.T) *tls.Config {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate test key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create test certificate: %v", err)
	}

	cert := tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  key,
	}

	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
		ClientAuth:         tls.NoClientCert,
		MinVersion:         tls.VersionTLS13,
	}
}

// recordingHandler records received OffsetNotify messages with thread-safe access.
type recordingHandler struct {
	mu       sync.Mutex
	received []*pb.OffsetNotify
	count    atomic.Int32
}

func (h *recordingHandler) HandleOffsetNotify(notify *pb.OffsetNotify) ([]*pb.NackNotify, error) {
	h.mu.Lock()
	h.received = append(h.received, notify)
	h.mu.Unlock()
	h.count.Add(1)
	return nil, nil
}

func (h *recordingHandler) HandleNack(_ *pb.NackNotify) error {
	return nil
}

func (h *recordingHandler) waitForCount(target int32, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if h.count.Load() >= target {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func (h *recordingHandler) getAll() []*pb.OffsetNotify {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make([]*pb.OffsetNotify, len(h.received))
	copy(cp, h.received)
	return cp
}

// staticLogReader stores effects by EffectRef key and implements LogReader.
type staticLogReader struct {
	mu   sync.RWMutex
	data map[pb.EffectSeq][]byte
}

func newStaticLogReader() *staticLogReader {
	return &staticLogReader{data: make(map[pb.EffectSeq][]byte)}
}

func (r *staticLogReader) Put(nodeID uint64, offset uint64, data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data[pb.NewEffectSeq(pb.NodeID(nodeID), offset)] = data
}

func (r *staticLogReader) ReadEffect(ref *pb.EffectRef) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key := pb.EffectSeqFromRef(ref)
	d, ok := r.data[key]
	if !ok {
		return nil, ErrPeerUnavailable
	}
	return d, nil
}

// mockLogReader returns fixed data for any ref. Used when fetch content doesn't matter.
type mockLogReader struct {
	data []byte
}

func (m *mockLogReader) ReadEffect(_ *pb.EffectRef) ([]byte, error) {
	return m.data, nil
}

// startNNodeCluster starts N peer managers with OS-assigned ports and wires them together.
// Blocks until all QUIC connections and heartbeat symmetry are established.
func startNNodeCluster(t *testing.T, handlers []EffectHandler, logReaders []LogReader) []*PeerManager {
	t.Helper()
	n := len(handlers)

	pms := make([]*PeerManager, n)
	pmCtx, pmCancel := context.WithCancel(context.Background())
	tlsCfg := generateTestTLSConfig(t)

	// Phase 1: bootstrap each node with just its own address (port 0)
	for i := range n {
		cfg := &ClusterConfig{
			NodeID: NodeId(i),
			Nodes: []NodeConfig{
				{ID: NodeId(i), Address: "127.0.0.1:0", Region: "test"},
			},
		}
		var pmErr error
		pms[i], pmErr = NewPeerManager(cfg, handlers[i], logReaders[i])
		if pmErr != nil {
			pmCancel()
			for j := range i {
				pms[j].Stop()
			}
			t.Fatalf("failed to create pm%d: %v", i, pmErr)
		}
		pms[i].serverTLS = tlsCfg
		pms[i].clientTLS = tlsCfg
		if err := pms[i].Start(pmCtx); err != nil {
			pmCancel()
			for j := range i {
				pms[j].Stop()
			}
			t.Fatalf("failed to start pm%d: %v", i, err)
		}
	}

	// Phase 2: collect actual addresses and update topologies
	nodes := make([]NodeConfig, n)
	for i := range n {
		nodes[i] = NodeConfig{
			ID:      NodeId(i),
			Address: pms[i].ListenAddr(),
			Region:  "test",
		}
	}
	for i := range n {
		pms[i].UpdateTopology(&ClusterConfig{
			NodeID: NodeId(i),
			Nodes:  nodes,
		})
	}

	// Phase 3: wait for all QUIC peer connections
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer waitCancel()

	for i := range n {
		for j := range n {
			if i == j {
				continue
			}
			if !pms[i].WaitForPeer(waitCtx, NodeId(j)) {
				pmCancel()
				for k := range n {
					pms[k].Stop()
				}
				t.Fatalf("pm%d did not connect to pm%d", i, j)
			}
		}
	}

	// Phase 4: wait for heartbeats to establish peer liveness and symmetry
	waitForHealthy := func() bool {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			allHealthy := true
			for i := range n {
				if pms[i].healthTable == nil {
					continue
				}
				for j := range n {
					if i == j {
						continue
					}
					ph := pms[i].healthTable.GetOrCreate(NodeId(j))
					if !ph.alive.Load() || !ph.symmetric.Load() {
						allHealthy = false
						break
					}
				}
				if !allHealthy {
					break
				}
			}
			if allHealthy {
				return true
			}
			time.Sleep(100 * time.Millisecond)
		}
		return false
	}
	if !waitForHealthy() {
		pmCancel()
		for k := range n {
			pms[k].Stop()
		}
		t.Fatal("peers did not become healthy within timeout")
	}

	t.Cleanup(func() {
		pmCancel()
		for i := range n {
			pms[i].Stop()
		}
	})

	return pms
}
