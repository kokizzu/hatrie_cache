#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH002(HashJoinBaseline|GraceHashJoin)$' -benchmem -benchtime=2s -count=5
