#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTU23StringMultikeyIndexBoundedBuild$' -benchtime=1x -benchmem -count=5
