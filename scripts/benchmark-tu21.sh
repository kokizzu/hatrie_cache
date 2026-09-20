#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
	-run '^$' \
	-bench '^BenchmarkTU21Migration(Status|Advance|Marshal|Unmarshal)$' \
	-benchmem \
	-count=5
