#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkSecondaryIndexPostingBuildC204$' -benchmem -count=3
