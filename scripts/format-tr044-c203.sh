#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPeer/compact_protocol.go hat/hatPeer/compact_compression.go hat/hatPeer/compact_compression_test.go hat/hatPeer/compact_compression_benchmark_test.go
