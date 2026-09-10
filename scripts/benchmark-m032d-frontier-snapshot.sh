#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLFrontierSnapshotProvider(Baseline)?$' -benchmem -benchtime=250ms -count=5
