#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench 'BenchmarkC222OrderedIndexMonotonicBuild$' -benchmem -benchtime=200ms -count=3
