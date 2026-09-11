#!/usr/bin/env bash
set -euo pipefail

test -s SQL_PACKED_NUMERIC_PREDICATE.md
rg -F 'SQL_PACKED_NUMERIC_PREDICATE.md' README.md
rg -F '## SQL Packed Numeric Predicate Kernel' BENCHMARK.md
rg -F 'CH-048' ENGINE_IDEAS.md
rg -F 'M065s: SQL packed numeric predicate kernel' CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
