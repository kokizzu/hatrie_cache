#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

gofmt -w \
  hat/hatPipeline/frontier_retention.go \
  hat/hatPipeline/frontier_retention_test.go \
  hat/hatPipeline/frontier_retention_benchmark_test.go
