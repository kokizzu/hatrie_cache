#!/usr/bin/env bash
set -euo pipefail

exec env GOMAXPROCS=1 go test ./hat/hatAuth -run '^$' -bench '^BenchmarkMU021BeforePolicyAuthorize$' -benchmem -benchtime=500ms -count=5
