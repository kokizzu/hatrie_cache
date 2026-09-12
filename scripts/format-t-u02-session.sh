#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
gofmt -w \
	"$root/hat/hatPeer/compact_session.go" \
	"$root/hat/hatPeer/compact_session_test.go" \
	"$root/hat/hatPeer/compact_session_benchmark_test.go"
