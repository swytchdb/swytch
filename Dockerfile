# Build stage
FROM --platform=$BUILDPLATFORM golang:1.26.5 AS builder

# Target platform args (automatically set by buildx)
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG EXTRA_BUILD

WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY beacon/ ./beacon/
COPY cache/ ./cache/
COPY cluster/ ./cluster/
COPY crdt/ ./crdt/
COPY effects/ ./effects/
COPY keytrie/ ./keytrie/
COPY metrics/ ./metrics/
COPY redis/ ./redis/
COPY sql/ ./sql/
COPY telemetry/ ./telemetry/
COPY tracing/ ./tracing/
COPY *.go .

# Build the binary for the target platform
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build ${EXTRA_BUILD} -ldflags="-s -w -X main.Version=${VERSION}" -o swytch .

# Runtime stage
FROM scratch

# Copy CA certificates for outbound HTTPS
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy binary from builder
COPY --from=builder /app/swytch /swytch

ENTRYPOINT ["/swytch"]
