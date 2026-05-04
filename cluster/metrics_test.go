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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func getCounterValue(c prometheus.Counter) float64 {
	var m dto.Metric
	c.Write(&m)
	return m.GetCounter().GetValue()
}

func getGaugeValue(g prometheus.Gauge) float64 {
	var m dto.Metric
	g.Write(&m)
	return m.GetGauge().GetValue()
}

func TestRecordNotificationSent(t *testing.T) {
	before := getCounterValue(NotificationsSentTotal)
	RecordNotificationSent()
	after := getCounterValue(NotificationsSentTotal)
	if after != before+1 {
		t.Fatalf("expected counter to increment by 1, got %f -> %f", before, after)
	}
}

func TestRecordNotificationReceived(t *testing.T) {
	before := getCounterValue(NotificationsReceivedTotal)
	RecordNotificationReceived()
	after := getCounterValue(NotificationsReceivedTotal)
	if after != before+1 {
		t.Fatalf("expected counter to increment by 1, got %f -> %f", before, after)
	}
}

func TestRecordFetchServed(t *testing.T) {
	before := getCounterValue(FetchesServedTotal)
	RecordFetchServed()
	after := getCounterValue(FetchesServedTotal)
	if after != before+1 {
		t.Fatalf("expected counter to increment by 1, got %f -> %f", before, after)
	}
}

func TestRecordPeerConnectedDisconnected(t *testing.T) {
	RecordPeerConnected(1)
	g := peerConnected.WithLabelValues("1")
	val := getGaugeValue(g)
	if val != 1 {
		t.Fatalf("expected peer_connected=1, got %f", val)
	}

	RecordPeerDisconnected(1)
	val = getGaugeValue(g)
	if val != 0 {
		t.Fatalf("expected peer_connected=0, got %f", val)
	}
}

func TestRecordNotificationDropped(t *testing.T) {
	RecordNotificationDropped(5)
	c := peerNotificationsDroppedTotal.WithLabelValues("5")
	var m dto.Metric
	c.Write(&m)
	if m.GetCounter().GetValue() < 1 {
		t.Fatal("expected notifications_dropped counter >= 1")
	}
}

func TestRecordPeerReconnect(t *testing.T) {
	RecordPeerReconnect(3)
	c := peerReconnectsTotal.WithLabelValues("3")
	var m dto.Metric
	c.Write(&m)
	if m.GetCounter().GetValue() < 1 {
		t.Fatal("expected reconnects counter >= 1")
	}
}
