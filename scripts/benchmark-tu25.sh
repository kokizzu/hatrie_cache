#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTU25' -benchmem -count=5
