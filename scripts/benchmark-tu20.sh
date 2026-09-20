#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
	-run '^$' \
	-bench '^BenchmarkTU20(BeforeMap(Get|Set)|AfterSpace(Get|Set|TargetGet)|AfterOneRecordBatchConversion)$' \
	-benchmem \
	-count=5
