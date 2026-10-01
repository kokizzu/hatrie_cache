#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTU22SeparateUniqueIndexes$' -benchmem -count=5
