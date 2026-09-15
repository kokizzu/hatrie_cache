#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench 'BenchmarkFrontierReadHold' -benchmem -count=5
