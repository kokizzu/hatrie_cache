#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'BenchmarkMZ029IncrementalIntervalJoin' -benchmem -count=5
