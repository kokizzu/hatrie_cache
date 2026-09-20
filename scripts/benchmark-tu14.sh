#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatTopology -run '^$' -bench '^BenchmarkTU14VShard(Route|PlanRebalance)$' -benchmem -count=5
