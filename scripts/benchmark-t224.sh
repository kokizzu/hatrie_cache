#!/usr/bin/env bash
set -euo pipefail

make benchmark-sql-partial-index
go test ./hat/hatDataStructure -run '^$' -bench 'Benchmark(ConditionalFunctionalIndex|TU24)' -benchmem -count=5
