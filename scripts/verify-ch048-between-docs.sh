#!/usr/bin/env bash
set -euo pipefail

rg -F 'CH-048' ENGINE_IDEAS.md CH048_BETWEEN_PREDICATE.md BENCHMARK.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
rg -F 'make benchmark-ch048-between' CH048_BETWEEN_PREDICATE.md BENCHMARK.md
rg -F 'NOT BETWEEN' CH048_BETWEEN_PREDICATE.md hat/hatSql/ch048_between_predicate_test.go
