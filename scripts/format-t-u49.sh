#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
gofmt -w hat/hatTopology/replica_hedging_test.go hat/hatTopology/replica_hedging_benchmark_test.go
