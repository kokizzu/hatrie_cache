#!/usr/bin/env bash
set -euo pipefail

mkdir -p build/benchmarks
go test ./hat/hatPagination -run '^$' -bench 'Benchmark(CursorTokenEncode|CursorTokenDecode|CursorTokenTextEncode|CursorTokenTextDecode|JSONCursorEncode)$' -benchmem -count=5 | tee build/benchmarks/tu42-pagination-token.txt
