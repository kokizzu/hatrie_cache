#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
	-run '^$' \
	-bench '^BenchmarkT233RegularTransactionReadBaseline$' \
	-benchmem \
	-count=5
