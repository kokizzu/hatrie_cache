#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH003(HashJoinBaseline|ParallelHashJoin)$' -benchtime=2s -benchmem -count=5
