#!/usr/bin/env bash
set -euo pipefail

test -f CH048_NUMERIC_IN.md
rg -q 'CH-048' CH048_NUMERIC_IN.md
rg -q 'BenchmarkCH048NumericINFastPath' CH048_NUMERIC_IN.md
rg -q 'ch048-numeric-in-predicates' BENCHMARK.md
rg -q 'CH-048' ENGINE_IDEAS.md
rg -q 'M065-numeric-in' CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
printf 'CH-048 numeric IN documentation verified\n'
