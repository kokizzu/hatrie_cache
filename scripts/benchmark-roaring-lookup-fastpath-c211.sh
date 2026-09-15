#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkRoaringBitmapLookupFastPathC211$' -benchmem -count=3
