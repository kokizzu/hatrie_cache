#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkUpsertBatchSmallVector(Fresh)?C210$' -benchmem -count=3
