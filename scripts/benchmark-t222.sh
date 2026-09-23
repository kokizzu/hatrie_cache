#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench 'BenchmarkSQLJSONMultikey' -benchmem -count=5
go test ./hat/hatDataStructure -run '^$' -bench 'Benchmark(StringMultikeyIndex|TupleMultikeyIndex)' -benchmem -count=5
