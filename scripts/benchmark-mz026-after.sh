#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ026AdaptiveSortedArrangement' -benchmem -count=5
