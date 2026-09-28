#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCodec -run '^$' -bench 'BenchmarkFloat64XOR' -benchmem -benchtime="${BENCHTIME:-200ms}" -count="${COUNT:-5}"
