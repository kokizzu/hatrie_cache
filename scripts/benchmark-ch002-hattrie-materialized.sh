#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkHatTrieSQLOrderedRange(Baseline|SparseMarks)$' -benchmem -count=5
