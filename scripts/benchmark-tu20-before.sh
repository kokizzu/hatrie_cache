#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
	-run '^$' \
	-bench '^BenchmarkTU20BeforeMap(Get|Set)$' \
	-benchmem \
	-count=5
