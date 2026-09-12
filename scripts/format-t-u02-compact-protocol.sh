#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
gofmt -w \
	"$root/hat/hatPeer/compact_protocol.go" \
	"$root/hat/hatPeer/compact_protocol_test.go" \
	"$root/hat/hatPeer/compact_protocol_benchmark_test.go"
