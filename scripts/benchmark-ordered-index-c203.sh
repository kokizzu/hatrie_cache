#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^(BenchmarkOrderedIndexPositionLookup|BenchmarkOrderedIndexSmallMutation)$' -benchmem -count=1
