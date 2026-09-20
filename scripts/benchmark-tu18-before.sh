#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
	-run '^$' \
	-bench '^BenchmarkTU18BeforeMap(Get|Set)$' \
	-benchmem \
	-count=5
