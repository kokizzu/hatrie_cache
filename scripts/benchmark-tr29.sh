#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkOrderedIndexDescending(MaterializeBaseline|MaterializeAllocBaseline|ReverseIterator)$' -benchmem -benchtime=200ms -count=5
