#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestTypedTable(Aggregate|Join)Arrangement'
go test -race ./hat/hatSql -run 'TestTypedTable(Aggregate|Join)Arrangement'
rg -q 'func \(arrangement \*TypedTableAggregateArrangement\) Hydrate' hat/hatSql/typed_table_arrangements.go
rg -q 'func \(arrangement \*TypedTableJoinArrangement\) Hydrate' hat/hatSql/typed_table_join_arrangements.go
rg -q '## Typed Arrangement Hydration' BENCHMARK.md
rg -q 'Bounded arrangement hydration' ADOPTED_QUERY_ENGINE_IDEAS.md
git diff --check -- \
  hat/hatSql/typed_table_arrangements.go \
  hat/hatSql/typed_table_join_arrangements.go \
  hat/hatSql/typed_table_arrangement_hydration_test.go \
  hat/hatSql/typed_table_arrangement_hydration_benchmark_test.go \
  scripts/test-typed-table-arrangement-hydration.sh \
  scripts/test-race-typed-table-arrangement-hydration.sh \
  scripts/format-typed-table-arrangement-hydration.sh \
  scripts/benchmark-typed-table-arrangement-hydration.sh \
  scripts/check-typed-table-arrangement-hydration.sh \
  scripts/deliver-typed-table-arrangement-hydration.sh \
  BENCHMARK.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md
