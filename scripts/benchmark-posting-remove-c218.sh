#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkC218PostingRemoveSorted$' -benchmem -count=5
