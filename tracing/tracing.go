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
	"fmt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
)

var enabled bool

// Config holds OpenTelemetry tracing configuration.
type Config struct {
	Endpoint    string // OTLP HTTP endpoint (e.g. http://localhost:4318). Empty = disabled.
	ServiceName string // OTel service name. Defaults to "swytch".
	NodeID      uint64 // Cluster node ID, added as a resource attribute.
	Insecure    bool   // Use HTTP instead of HTTPS for the OTLP endpoint.
}

// Init sets up the global OTel TracerProvider. When Endpoint is empty it
// registers a no-op provider (zero overhead). Returns a shutdown function
// that must be called on process exit to flush pending spans.
func Init(ctx context.Context, cfg Config) func(context.Context) error {
	if cfg.Endpoint == "" {
		// No-op: leave the default global provider (which is already no-op).
		return func(context.Context) error { return nil }
	}

	if cfg.ServiceName == "" {
		cfg.ServiceName = "swytch"
	}

	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(cfg.Endpoint),
	}
	if cfg.Insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}

	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		slog.Error("tracing: failed to create OTLP exporter", "error", err)
		return func(context.Context) error { return nil }
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceInstanceID(fmt.Sprintf("node-%d", cfg.NodeID)),
		),
	)
	if err != nil {
		slog.Error("tracing: failed to create resource", "error", err)
		return func(context.Context) error { return exporter.Shutdown(ctx) }
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	enabled = true
	slog.Info("tracing: initialized", "endpoint", cfg.Endpoint, "service", cfg.ServiceName, "node", cfg.NodeID)

	return tp.Shutdown
}

// Enabled returns whether tracing was initialized with a real exporter.
func Enabled() bool {
	return enabled
}

// Tracer returns the package-level tracer for swytch instrumentation.
func Tracer() trace.Tracer {
	return otel.Tracer("swytch")
}

// Binary trace context format: [1B version][16B traceID][8B spanID][1B traceFlags] = 26 bytes
const traceContextSize = 26

// InjectIntoBytes serializes the span context from ctx into a compact 26-byte
// binary representation. Returns nil if the span context is invalid.
func InjectIntoBytes(ctx context.Context) []byte {
	sc := trace.SpanFromContext(ctx).SpanContext()
	if !sc.IsValid() {
		return nil
	}

	buf := make([]byte, traceContextSize)
	buf[0] = 0 // version
	tid := sc.TraceID()
	copy(buf[1:17], tid[:])
	sid := sc.SpanID()
	copy(buf[17:25], sid[:])
	buf[25] = byte(sc.TraceFlags())
	return buf
}

// ExtractFromBytes deserializes a 26-byte binary trace context and returns
// a Go context with the remote span context attached. Returns
// context.Background() if data is nil or malformed.
func ExtractFromBytes(data []byte) context.Context {
	if len(data) != traceContextSize {
		return context.Background()
	}

	var tid trace.TraceID
	copy(tid[:], data[1:17])
	var sid trace.SpanID
	copy(sid[:], data[17:25])
	flags := trace.TraceFlags(data[25])

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: flags,
		Remote:     true,
	})

	return trace.ContextWithRemoteSpanContext(context.Background(), sc)
}

// TracingHandler wraps an slog.Handler to inject trace_id and span_id
// attributes from the context into every log record.
type TracingHandler struct {
	inner slog.Handler
}

// NewTracingHandler creates a new TracingHandler wrapping the given handler.
func NewTracingHandler(inner slog.Handler) *TracingHandler {
	return &TracingHandler{inner: inner}
}

func (h *TracingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *TracingHandler) Handle(ctx context.Context, r slog.Record) error {
	sc := trace.SpanFromContext(ctx).SpanContext()
	if sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.inner.Handle(ctx, r)
}

func (h *TracingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &TracingHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *TracingHandler) WithGroup(name string) slog.Handler {
	return &TracingHandler{inner: h.inner.WithGroup(name)}
}
