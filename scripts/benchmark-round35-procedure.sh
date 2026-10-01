#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatProcedure -run '^$' -bench 'Benchmark(Direct|Registry)' -benchmem -count=5
