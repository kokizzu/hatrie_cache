#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM210TypedTableSnapshotAt(Baseline|SingleBaseline)$' -benchmem -count=5
