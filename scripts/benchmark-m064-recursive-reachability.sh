#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkRecursiveReachabilityMaintenance$' -benchmem -benchtime=100ms -count=5
