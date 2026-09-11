#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql \
	-run '^$' \
	-bench '^BenchmarkMZ015(BaselineManual|Normalize)$' \
	-benchmem \
	-count=5
