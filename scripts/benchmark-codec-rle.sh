#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCodec -run '^$' -bench 'BenchmarkRunLengthUint64' -benchmem -benchtime="${BENCHTIME:-200ms}" -count="${COUNT:-5}"
