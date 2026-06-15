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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// --- 9.3 Throughput ---

var (
	WritesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_writes_total",
		Help: "Total local write effects emitted",
	})

	ReadsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_reads_total",
		Help: "Total reads served",
	})

	ReadsLocalTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_reads_local_total",
		Help: "Reads served from local log/cache (no fetch)",
	})

	ReadsRemoteFetchTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_reads_remote_fetch_total",
		Help: "Reads that required a remote fetch",
	})

	NotificationsSentTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_notifications_sent_total",
		Help: "OffsetNotify messages broadcast",
	})

	NotificationsReceivedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_notifications_received_total",
		Help: "OffsetNotify messages received",
	})

	FetchesServedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_fetches_served_total",
		Help: "Fetch RPCs served to peers",
	})

	BindsEmittedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_binds_emitted_total",
		Help: "Bind effects emitted (concurrent writes detected)",
	})

	SnapshotsEmittedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_snapshots_emitted_total",
		Help: "Snapshot effects emitted (bind resolution)",
	})
)

// --- 9.4 Disk ---

var (
	SegmentActiveBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cluster_segment_active_bytes",
		Help: "Bytes used in the current live segment",
	})

	SegmentActiveSlots = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cluster_segment_active_slots",
		Help: "Slots used in the current live segment (out of 1M)",
	})

	SegmentsSealedTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cluster_segments_sealed_total",
		Help: "Number of sealed segments on this node",
	})

	DiskUsedBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cluster_disk_used_bytes",
		Help: "Total disk used by all log segments",
	})

	DiskCapacityBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cluster_disk_capacity_bytes",
		Help: "Total disk capacity",
	})

	DiskUsageRatio = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cluster_disk_usage_ratio",
		Help: "Disk usage ratio (used / capacity)",
	})
)

// --- 9.5 Peer Health ---

var (
	peerConnected = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cluster_peer_connected",
		Help: "1 if stream is up, 0 if down",
	}, []string{"peer"})

	peerReconnectsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cluster_peer_reconnects_total",
		Help: "Number of reconnections",
	}, []string{"peer"})

	peerNotificationsDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cluster_peer_notifications_dropped_total",
		Help: "Notifications dropped due to buffer full or disconnected peer",
	}, []string{"peer"})
)

// --- 9.6 Heartbeat & UDP Fast Path ---

var (
	heartbeatsSentTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_heartbeats_sent_total",
		Help: "Total heartbeat packets sent",
	})

	heartbeatsReceivedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_heartbeats_received_total",
		Help: "Total heartbeat packets received",
	})

	peerSymmetricGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cluster_peer_symmetric",
		Help: "1 if peer path is symmetric, 0 if asymmetric",
	}, []string{"peer"})

	peerAliveGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cluster_peer_alive",
		Help: "1 if peer is alive (heartbeat within timeout), 0 if dead",
	}, []string{"peer"})

	peerRttMsGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cluster_peer_rtt_ms",
		Help: "Estimated RTT to peer in milliseconds (from heartbeat)",
	}, []string{"peer"})

	udpNotifyAckLatencyMs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "cluster_udp_notify_ack_latency_ms",
		Help:    "Latency of UDP notification ACKs in milliseconds",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100},
	})
)

// RecordHeartbeatsSent increments the heartbeat sent counter.
func RecordHeartbeatsSent() {
	heartbeatsSentTotal.Inc()
}

// RecordHeartbeatReceived increments the heartbeat received counter.
func RecordHeartbeatReceived() {
	heartbeatsReceivedTotal.Inc()
}

// RecordPeerSymmetric updates the symmetry gauge for a peer.
func RecordPeerSymmetric(peerID NodeId, symmetric bool) {
	v := float64(0)
	if symmetric {
		v = 1
	}
	peerSymmetricGauge.WithLabelValues(peerLabel(peerID)).Set(v)
}

// RecordPeerAlive updates the alive gauge for a peer.
func RecordPeerAlive(peerID NodeId, alive bool) {
	v := float64(0)
	if alive {
		v = 1
	}
	peerAliveGauge.WithLabelValues(peerLabel(peerID)).Set(v)
}

// RecordPeerRTT updates the RTT gauge for a peer.
func RecordPeerRTT(peerID NodeId, rttNanos int64) {
	peerRttMsGauge.WithLabelValues(peerLabel(peerID)).Set(float64(rttNanos) / 1e6)
}

// RecordUDPNotifyACKLatency records the latency of a notification ACK.
func RecordUDPNotifyACKLatency(latencyMs float64) {
	udpNotifyAckLatencyMs.Observe(latencyMs)
}

// --- 9.7 Retransmission ---

var (
	retransmissionGiveUpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cluster_retransmission_giveups_total",
		Help: "Total retransmission give-ups (max retries exhausted) per peer",
	}, []string{"peer"})
)

// RecordRetransmissionGiveUp increments the give-up counter for a peer.
func RecordRetransmissionGiveUp(peerID NodeId) {
	retransmissionGiveUpsTotal.WithLabelValues(peerLabel(peerID)).Inc()
}

// --- Inline metric update functions ---

// RecordNotificationSent increments the sent counter for a broadcast.
func RecordNotificationSent() {
	NotificationsSentTotal.Inc()
}

// RecordNotificationReceived increments the received counter.
func RecordNotificationReceived() {
	NotificationsReceivedTotal.Inc()
}

// --- 9.10 QUIC Transport ---

var (
	quicStreamsOpenedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_quic_streams_opened_total",
		Help: "Total QUIC uni-streams opened for notification/heartbeat sends",
	})

	quicStreamErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_quic_stream_errors_total",
		Help: "Total QUIC uni-stream errors (open or read failures)",
	})
)

// RecordQUICStreamOpened increments the QUIC stream opened counter.
func RecordQUICStreamOpened() {
	quicStreamsOpenedTotal.Inc()
}

// RecordQUICStreamError increments the QUIC stream error counter.
func RecordQUICStreamError() {
	quicStreamErrorsTotal.Inc()
}

// RecordFetchServed increments the fetch served counter.
func RecordFetchServed() {
	FetchesServedTotal.Inc()
}

// RecordNotificationDropped increments the dropped notification counter for a peer.
func RecordNotificationDropped(peerID NodeId) {
	peerNotificationsDroppedTotal.WithLabelValues(peerLabel(peerID)).Inc()
}

// RecordPeerConnected sets the peer connection gauge to 1.
func RecordPeerConnected(peerID NodeId) {
	peerConnected.WithLabelValues(peerLabel(peerID)).Set(1)
}

// RecordPeerDisconnected sets the peer connection gauge to 0.
func RecordPeerDisconnected(peerID NodeId) {
	peerConnected.WithLabelValues(peerLabel(peerID)).Set(0)
}

// RecordPeerReconnect increments the reconnection counter for a peer.
func RecordPeerReconnect(peerID NodeId) {
	peerReconnectsTotal.WithLabelValues(peerLabel(peerID)).Inc()
}

func peerLabel(peerID NodeId) string {
	return fmt.Sprintf("%d", peerID)
}
