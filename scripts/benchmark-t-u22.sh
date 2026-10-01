#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTU22(SeparateUniqueIndexes|UniqueConstraintSet)$' -benchmem -count=5
