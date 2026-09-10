#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM065RankWindow/(full_recompute|incremental_append|mutable_update)$' -benchmem -count=5
