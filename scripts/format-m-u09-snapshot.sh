#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

gofmt -w \
  hat/hatPipeline/frontier_registry.go \
  hat/hatPipeline/frontier_snapshot.go \
  hat/hatPipeline/frontier_snapshot_test.go \
  hat/hatPipeline/frontier_snapshot_benchmark_test.go
