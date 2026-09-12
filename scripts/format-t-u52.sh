#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
gofmt -w hat/hatPeer/connection_pool.go hat/hatPeer/adaptive_breaker_test.go
