#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache \
	-run '^$' \
	-bench '^BenchmarkMZ012(BaselineAtLeastOnceSinkBatch100|ExactlyOnceSinkBatch100)$' \
	-benchmem \
	-count=5
