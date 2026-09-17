#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench='^BenchmarkSQLMutationDependencyGraph(ClaimReady|Save)$' -benchmem -count=5 -benchtime=100ms
