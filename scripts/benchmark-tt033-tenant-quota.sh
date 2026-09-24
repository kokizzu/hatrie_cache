#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatFiber -run '^$' -bench 'BenchmarkTT033' -benchmem -count=5
