#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '=== repository ==='
git status --short
git log -8 --oneline

printf '%s\n' '=== open backlog ==='
rg -n -A 8 -B 2 'M052i|M052j|M090|T042|T103|T150' INSPIRATION.md

printf '%s\n' '=== recent benchmark/doc tails ==='
tail -n 100 BENCHMARK.md
tail -n 100 SQL_DATAFLOW_EXECUTOR.md

printf '%s\n' '=== adopted idea insertion point ==='
rg -n -A 4 -B 3 'Constant folding|Compact hash aggregation|Vectorized blocks' ADOPTED_QUERY_ENGINE_IDEAS.md

printf '%s\n' '=== SQL expression AST ==='
rg -n -A 75 'type sqlExpr struct' hat/hatSql/query.go

printf '%s\n' '=== aggregate and HAVING helpers ==='
rg -n 'func sql.*[Ee]xpression.*[Ee]qual|sqlExpressionValuesStructurallyEqual' hat/hatSql/*.go
