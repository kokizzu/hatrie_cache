#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench 'BenchmarkTU55' -benchmem -count=5
