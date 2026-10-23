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

package tracing

import (
	"context"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestInjectExtractRoundtrip(t *testing.T) {
	tid := trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	sid := trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}
	flags := trace.FlagsSampled

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: flags,
		Remote:     true,
	})

	ctx := trace.ContextWithRemoteSpanContext(context.Background(), sc)
	// Wrap in a non-recording span so SpanFromContext returns something valid.
	ctx = trace.ContextWithSpan(ctx, nonRecordingSpan{sc: sc})

	data := InjectIntoBytes(ctx)
	if data == nil {
		t.Fatal("InjectIntoBytes returned nil for valid span context")
	}

	if len(data) != traceContextSize {
		t.Fatalf("expected %d bytes, got %d", traceContextSize, len(data))
	}

	extracted := ExtractFromBytes(data)
	esc := trace.SpanContextFromContext(extracted)

	if esc.TraceID() != tid {
		t.Errorf("TraceID mismatch: got %s, want %s", esc.TraceID(), tid)
	}

	if esc.SpanID() != sid {
		t.Errorf("SpanID mismatch: got %s, want %s", esc.SpanID(), sid)
	}

	if esc.TraceFlags() != flags {
		t.Errorf("TraceFlags mismatch: got %v, want %v", esc.TraceFlags(), flags)
	}

	if !esc.IsRemote() {
		t.Error("expected remote span context")
	}
}

func TestInjectIntoBytes_InvalidContext(t *testing.T) {
	data := InjectIntoBytes(context.Background())
	if data != nil {
		t.Errorf("expected nil for invalid context, got %d bytes", len(data))
	}
}

func TestExtractFromBytes_Nil(t *testing.T) {
	ctx := ExtractFromBytes(nil)

	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		t.Error("expected invalid span context from nil data")
	}
}

func TestExtractFromBytes_WrongSize(t *testing.T) {
	ctx := ExtractFromBytes([]byte{1, 2, 3})

	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		t.Error("expected invalid span context from short data")
	}
}

func TestTracingHandler_AddsTraceAttrs(t *testing.T) {
	var captured []slog.Attr

	inner := &capturingHandler{attrs: &captured}
	handler := NewTracingHandler(inner)

	tid := trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	sid := trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
		Remote:     false,
	})
	ctx := trace.ContextWithSpan(context.Background(), nonRecordingSpan{sc: sc})

	record := slog.Record{}
	record.AddAttrs(slog.String("msg", "test"))

	err := handler.Handle(ctx, record)
	if err != nil {
		t.Fatal(err)
	}

	foundTraceID := false
	foundSpanID := false

	for _, a := range captured {
		if a.Key == "trace_id" {
			foundTraceID = true
		}

		if a.Key == "span_id" {
			foundSpanID = true
		}
	}

	if !foundTraceID {
		t.Error("trace_id not found in log record")
	}

	if !foundSpanID {
		t.Error("span_id not found in log record")
	}
}

func TestTracingHandler_NoTraceAttrsWithoutSpan(t *testing.T) {
	var captured []slog.Attr

	inner := &capturingHandler{attrs: &captured}
	handler := NewTracingHandler(inner)

	record := slog.Record{}
	record.AddAttrs(slog.String("msg", "test"))

	err := handler.Handle(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}

	for _, a := range captured {
		if a.Key == "trace_id" || a.Key == "span_id" {
			t.Errorf("unexpected tracing attr %q in record without span", a.Key)
		}
	}
}

func TestInitNoOp(t *testing.T) {
	shutdown := Init(context.Background(), Config{})
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// --- test helpers ---

// nonRecordingSpan implements trace.Span with a fixed SpanContext.
type nonRecordingSpan struct {
	trace.Span

	sc trace.SpanContext
}

func (s nonRecordingSpan) SpanContext() trace.SpanContext { return s.sc }
func (s nonRecordingSpan) IsRecording() bool              { return false }

// capturingHandler captures all attributes added to records.
type capturingHandler struct {
	attrs *[]slog.Attr
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	r.Attrs(func(a slog.Attr) bool {
		*h.attrs = append(*h.attrs, a)

		return true
	})

	return nil
}
func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }
