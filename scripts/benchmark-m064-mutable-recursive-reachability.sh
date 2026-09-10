#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkRecursiveReachabilityMaintenance/(incremental_append|full_recompute_delete|full_recompute_update|mutable_delete|mutable_update)$' -benchmem -count=5
