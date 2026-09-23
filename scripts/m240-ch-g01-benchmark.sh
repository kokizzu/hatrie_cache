#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH001(HashJoinBaseline|PartialMergeJoin)$' -benchmem -benchtime=2s -count=5
