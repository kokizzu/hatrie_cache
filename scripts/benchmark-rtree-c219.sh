#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkRTreeSearchSmallResultC219$' -benchmem -count=5
