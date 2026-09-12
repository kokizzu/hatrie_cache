#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
gofmt -w hat/hatPeer/compact_listener.go hat/hatPeer/compact_listener_test.go hat/hatPeer/compact_listener_benchmark_test.go
