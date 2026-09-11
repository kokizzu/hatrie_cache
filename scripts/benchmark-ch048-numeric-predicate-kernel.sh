#!/usr/bin/env bash
set -euo pipefail

GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableColumnarCompressedBatchesQuery/(legacy|compressed)$' -benchmem -benchtime=250ms -count=7
