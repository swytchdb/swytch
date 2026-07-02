# CloxCache build and test commands

set shell := ["bash", "-uc"]

BINARY_NAME := "swytch"
VERSION := `git describe --tags --always --dirty 2>/dev/null || echo "dev"`

# Default recipe - show available commands
default:
    @just --list

# Install Go dependencies
deps:
    go mod download

# Build the server binary
build:
    go build -o {{BINARY_NAME}} .

# Build with release flags (stripped, trimpath)
build-release:
    CGO_ENABLED=0 go build \
        -ldflags="-s -w -X main.Version={{VERSION}}" \
        -trimpath \
        -o {{BINARY_NAME}} \
        .

# Build for a specific platform
build-platform goos goarch:
    CGO_ENABLED=0 GOOS={{goos}} GOARCH={{goarch}} go build \
        -ldflags="-s -w -X main.Version={{VERSION}}" \
        -trimpath \
        -o dist/{{BINARY_NAME}}-{{VERSION}}-{{goos}}-{{goarch}} \
        .

# Run all tests
test:
    go test -v ./...
    go test -v -tags effects ./...

# Run all tests with race detector (required for CI)
test-race:
    go test -race -v ./...
    go test -race -v -tags effects ./...

# Run tests for a specific package
test-pkg pkg:
    go test -v ./{{pkg}}

# Run a single test by name
test-run name pkg=".":
    go test -v -run {{name}} ./{{pkg}}

# Run benchmarks
bench:
    cd benchmarks && go test -v -run TestCapacitySweep

# Start Redis-compatible server
redis port="6379" maxmemory="268435456":
    ./{{BINARY_NAME}} redis --port {{port}} --maxmemory {{maxmemory}}

# Start Redis with persistence
redis-persistent port="6379" db-path="/data/redis.db":
    ./{{BINARY_NAME}} redis --persistent --port {{port}} --db-path {{db-path}}

# Start Memcached-compatible server
memcached memory="256" port="11211":
    ./{{BINARY_NAME}} memcached -m {{memory}} -p {{port}}

# Clean build artifacts
clean:
    rm -f {{BINARY_NAME}}
    rm -rf dist/
    go clean

# Generate protobuf code
proto:
    protoc --go_out=. --go_opt=paths=source_relative \
           --go-grpc_out=. --go-grpc_opt=paths=source_relative \
           cluster/proto/replication.proto \
           cluster/proto/dataplane/dataplane.proto

# Format code
fmt:
    go fmt ./...

# Run linter
lint:
    go vet ./...

# Build blog draft PDFs from markdown using pandoc
blog-pdfs:
    for f in docs/blog/drafts/*.md; do \
        pandoc "$f" -o "${f%.md}.pdf"; \
    done

# Build documentation (requires Hugo)
docs:
    cd docs && hugo --minify

# Serve documentation locally
docs-serve:
    cd docs && hugo server

# Generate checksums for dist files
checksums:
    cd dist && sha256sum * > checksums.txt

# Build and run Redis server
run: build redis

# CI workflow - what runs on PRs
ci: deps test test-race
