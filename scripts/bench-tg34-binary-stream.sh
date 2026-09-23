#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatHttp -run '^$' -bench '^BenchmarkBinaryStreamFraming$' -benchmem -benchtime=100ms -count=5
