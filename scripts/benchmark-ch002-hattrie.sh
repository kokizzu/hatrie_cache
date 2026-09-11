#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkHatTrieSQLOrderedRange(Stream)?(Baseline|SparseMarks)$' -benchmem -count=5
