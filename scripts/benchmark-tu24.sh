#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
	-run '^$' \
	-bench '^BenchmarkTU24(BeforeConditional(Upsert|Lookup)|AfterCatalog(Upsert|Lookup))$' \
	-benchmem \
	-count=5
