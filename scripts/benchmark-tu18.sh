#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
	-run '^$' \
	-bench '^BenchmarkTU18(BeforeMap(Get|Set)|AfterVolatileCache(Get|Peek|Set|GetTTL))$' \
	-benchmem \
	-count=5
