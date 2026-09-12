#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
gofmt -w hat/hatReplication/rpo_status.go hat/hatReplication/rpo_status_test.go hat/hatReplication/rpo_status_benchmark_test.go
