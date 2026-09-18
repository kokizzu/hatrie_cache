#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLMutationDependencyGraph(ClaimReady|Save)$' -benchmem -count=5
