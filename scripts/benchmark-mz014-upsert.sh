#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
	-run '^$' \
	-bench '^BenchmarkMZ014(BaselineMapLastWrite|BaselineMapLastWriteReuse|UpsertBatchLastWrite|UpsertBatchFresh)$' \
	-benchmem \
	-count=5
