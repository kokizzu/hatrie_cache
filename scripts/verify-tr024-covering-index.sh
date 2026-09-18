#!/usr/bin/env bash
set -euo pipefail

test -f TR024_COVERING_INDEX.md
test -f hat/hatSchema/tr024_covering_index_test.go
test -f hat/hatSchema/tr024_covering_index_benchmark_test.go
rg -q 'BuildCoveringIndex' hat/hatSchema/materialized.go
rg -q 'ResolveSQLCoveringSource' hat/hatSchema/materialized.go
rg -q 'TR-24' INSPIRATION_BACKLOG.md
rg -q 'TR-024' BENCHMARK.md
