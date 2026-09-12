#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

gofmt -w \
  hat/hatPeer/peer_stream.go \
  hat/hatPeer/peer_stream_test.go \
  hat/hatPeer/peer_stream_benchmark_test.go
