#!/usr/bin/env sh
set -eu

git diff --check -- Makefile BENCHMARK.md INDEX_PROPOSAL.md hat/hatCache/sql_typed_index_baseline_benchmark_test.go scripts/bench-sql-typed-index-baseline.sh scripts/deliver-sql-typed-index-baseline.sh scripts/inspect-sql-indexes.sh
git add -- Makefile BENCHMARK.md INDEX_PROPOSAL.md hat/hatCache/sql_typed_index_baseline_benchmark_test.go scripts/bench-sql-typed-index-baseline.sh scripts/deliver-sql-typed-index-baseline.sh scripts/inspect-sql-indexes.sh
git commit -m "bench(sql): add typed index baseline"
git push
