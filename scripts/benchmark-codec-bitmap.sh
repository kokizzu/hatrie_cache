#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCodec -run '^$' -bench 'BenchmarkBoolBitmap' -benchmem -benchtime="${BENCHTIME:-200ms}" -count="${COUNT:-5}"
