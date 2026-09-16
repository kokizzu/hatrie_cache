#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline \
	-run '^$' \
	-bench '^BenchmarkMZ007FrontierBackpressure' \
	-benchmem \
	-benchtime=200ms \
	-count=5
