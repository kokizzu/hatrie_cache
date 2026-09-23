#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkT229SpacePutBaseline$' -benchmem -count=5 -cpu=1
