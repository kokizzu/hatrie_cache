#!/usr/bin/env bash
set -euo pipefail

for path in \
  SQL_PACKED_BOOLEAN_PREDICATE.md \
  README.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  INSPIRATION.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md; do
  test -s "$path"
done

rg -F -q 'SQL_PACKED_BOOLEAN_PREDICATE.md' README.md
rg -F -q 'M065t: SQL Packed Boolean Predicate Kernel' BENCHMARK.md
rg -F -q '3.12x CPU' BENCHMARK.md
rg -F -q 'M065t' INSPIRATION.md
rg -F -q 'Packed boolean predicate evaluation' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -F -q 'packed-boolean predicates' ENGINE_IDEAS.md
rg -F -q 'M065t: SQL packed boolean predicate kernel' CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
test ! -e scripts/inspect-packed-bool-sql.sh
test ! -e scripts/inspect-bool-helpers.sh
test ! -e scripts/inspect-m065t-context.sh
test ! -e scripts/inspect-inspiration-tail.sh
