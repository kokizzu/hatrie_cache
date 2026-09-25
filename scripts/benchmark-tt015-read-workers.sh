#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^$' -bench 'BenchmarkTT015Prefetch' -benchtime=100ms -count=5 -benchmem
