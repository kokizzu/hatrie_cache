#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkC216FunctionalLookupIDs$' -benchmem -count=5
