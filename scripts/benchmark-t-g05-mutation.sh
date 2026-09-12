#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkOrderedIndexMutation$' -benchmem -count=5 -benchtime=200ms
