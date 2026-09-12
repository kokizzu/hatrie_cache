#!/usr/bin/env bash
set -euo pipefail

go test -run '^$' -bench '^BenchmarkOrderedIndexTraversal$' -benchmem -benchtime=200ms -count=5 ./hat/hatDataStructure
