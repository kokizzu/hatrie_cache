#!/usr/bin/env bash
set -euo pipefail

git add Makefile ENGINE_IDEAS.md INSPIRATION.md README.md BENCHMARK.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md SQL_PACKED_NUMERIC_PREDICATE.md hat/hatSql/columnar_numeric_predicate.go hat/hatSql/columnar_numeric_predicate_test.go hat/hatSql/query.go scripts/benchmark-ch048-numeric-predicate-kernel-baseline.sh scripts/benchmark-ch048-numeric-predicate-kernel.sh scripts/test-ch048-numeric-predicate-kernel.sh scripts/test-ch048-numeric-predicate-kernel-full.sh scripts/race-ch048-numeric-predicate-kernel.sh scripts/vet-ch048-numeric-predicate-kernel.sh scripts/verify-ch048-numeric-predicate-kernel-docs.sh scripts/format-ch048-numeric-predicate-kernel.sh scripts/review-ch048-numeric-predicate-kernel.sh scripts/commit-ch048-numeric-predicate-kernel.sh scripts/push-ch048-numeric-predicate-kernel.sh
git commit -m 'perf(sql): add packed numeric predicate kernel'
