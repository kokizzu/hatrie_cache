#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkCHU48RankedTokenPostings$' -benchmem -count=5
