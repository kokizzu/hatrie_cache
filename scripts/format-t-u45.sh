#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
gofmt -w hat/hatMetrics/space_operation_stats.go hat/hatMetrics/space_operation_stats_test.go
