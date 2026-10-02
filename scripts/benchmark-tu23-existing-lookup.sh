#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkStringMultikeyIndexLookup$' -benchmem -count=5
