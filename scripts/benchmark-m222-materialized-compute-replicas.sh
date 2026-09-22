#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM222MaterializedComputeReplicaRefresh$' -benchmem -benchtime=100ms -count=5
