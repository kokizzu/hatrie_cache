#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkHatTrieSQLOrderedRangeStream(Baseline|SparseMarks)$' -benchmem -count=5
