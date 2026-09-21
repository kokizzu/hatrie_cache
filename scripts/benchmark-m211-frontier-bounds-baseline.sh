#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM211TypedTableSnapshotAtBaseline$' -benchmem -count=5
