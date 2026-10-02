#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkStringMultikeyIndexBuild$' -benchtime=1x -benchmem -count=5
