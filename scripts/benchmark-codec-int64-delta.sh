#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCodec -run '^$' -bench 'BenchmarkInt64Delta' -benchmem -benchtime="${BENCHTIME:-200ms}" -count="${COUNT:-5}"
