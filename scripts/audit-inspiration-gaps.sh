#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

if [[ "${1:-all}" == "backlog" ]]; then
  for file in PRODUCT_IDEA_GAPS.md INSPIRATION_BACKLOG.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md; do
    if [[ -f "$file" ]]; then
      printf '\n=== %s ===\n' "$file"
      sed -n '1,320p' "$file"
    fi
  done
  exit 0
fi

if [[ "${1:-all}" == "rows" ]]; then
  rg -n '^\| (CH-U|MZ-|TR-)' PRODUCT_IDEA_GAPS.md || true
  printf '\n=== Materialize/Tarantool proposal headings and rows ===\n'
  rg -n '^#{2,3} |^\|.*(Materialize|Tarantool|MZ-|TR-)' PRODUCT_IDEA_GAPS.md INSPIRATION_BACKLOG.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md || true
  exit 0
fi

if [[ "${1:-all}" == "materialize" ]]; then
  rg -n '^\| MZ-|^\|.*Materialize' PRODUCT_IDEA_GAPS.md INSPIRATION_BACKLOG.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md || true
  exit 0
fi

if [[ "${1:-all}" == "tarantool" ]]; then
  rg -n '^\| TR-|^\|.*Tarantool' PRODUCT_IDEA_GAPS.md INSPIRATION_BACKLOG.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md || true
  exit 0
fi

if [[ "${1:-all}" == "explain" ]]; then
  printf '%s\n' '=== Explain and plan APIs ==='
  rg -n -m 240 '^type .*Explain|^type .*Plan|^func .*Explain|^func .*Plan|EXPLAIN|Explain' hat/hatSql hat/hatCache --glob '*.go' || true
  printf '%s\n' '=== Explain-related tests and docs ==='
  rg --files hat/hatSql hat/hatCache | rg -i 'explain|plan' | sort || true
  rg -n -m 160 'cost|memory|frontier|arrangement|operator' --glob '*EXPLAIN*.md' --glob '*PLAN*.md' --glob 'MZ*.md' . || true
  exit 0
fi

if [[ "${1:-all}" == "explain-doc" ]]; then
  sed -n '1,280p' SQL_EXPLAIN_OPTIMIZER.md
  printf '%s\n' '=== Explain source files ==='
  rg --files hat/hatSql hat/hatCache | rg -i 'explain|planner|statistics|whatif' | sort
  printf '%s\n' '=== Explain type and option declarations ==='
  rg -n -A 24 -B 8 '^type SQLExplain|^type SQLQueryOptions|^type SQLPlanner|^func .*Explain' hat/hatSql hat/hatCache --glob '*.go' || true
  exit 0
fi

if [[ "${1:-all}" == "explain-core" ]]; then
  for file in hat/hatSql/explain.go hat/hatSql/explain_dataflow.go hat/hatSql/explain_pipeline.go hat/hatSql/mu012_arrangement_explain.go hat/hatSql/explain_format.go; do
    if [[ -f "$file" ]]; then
      printf '\n=== %s ===\n' "$file"
      rg -n -A 80 -B 12 '^type ExplainStep|^type SQLArrangement|^func sqlExplain|^func .*Explain|^func .*Arrangement' "$file" || true
    fi
  done
  exit 0
fi

if [[ "${1:-all}" == "explain-type" ]]; then
  rg -n -A 70 -B 12 '^type ExplainStep|^type QueryResult|^type SQLQueryOptions|^type SQLExplainStep' hat/hatSql hat/hatCache --glob '*.go' || true
  exit 0
fi

if [[ "${1:-all}" == "explain-implementation" ]]; then
  rg -n -A 100 -B 20 '^func sqlExplainStepsWithResolver|^func sqlExplainStep|^func execute.*Explain|^func .*explain.*Step|^func .*Explain.*Query' hat/hatSql --glob '*.go' || true
  exit 0
fi

if [[ "${1:-all}" == "arrangement-cost" ]]; then
  rg -n -A 90 -B 18 '^type SQLArrangementCost|^func NewSQLArrangementCost|^func \(.*SQLArrangementCost|^type SQLArrangementWorkload' hat/hatSql --glob '*.go' || true
  exit 0
fi

if [[ "${1:-all}" == "explain-parser" ]]; then
  rg -n -A 70 -B 18 'explainAnalyze|EXPLAIN ANALYZE|parse.*Explain|parse.*EXPLAIN|\.explain' hat/hatSql --glob '*.go' --glob '!**/*_test.go' || true
  exit 0
fi

if [[ "${1:-all}" == "explain-symbols" ]]; then
  rg -n -m 120 'func parseSQLQuery|func parseQuery|explainAnalyze|explainPipeline|explainFormat|EXPLAIN' hat/hatSql --glob '*.go' --glob '!**/*_test.go' || true
  exit 0
fi

if [[ "${1:-all}" == "explain-parser-core" ]]; then
  sed -n '5535,5635p' hat/hatSql/query.go
  exit 0
fi

if [[ "${1:-all}" == "explain-branch" ]]; then
  sed -n '820,925p' hat/hatSql/query.go
  sed -n '13070,13155p' hat/hatSql/query.go
  exit 0
fi

if [[ "${1:-all}" == "explain-function" ]]; then
  rg -n -A 180 -B 15 '^func explainSQLQuery' hat/hatSql --glob '*.go' || true
  exit 0
fi

if [[ "${1:-all}" == "explain-merge" ]]; then
  rg -n -A 90 -B 18 '^func sqlMergeExplainCardinalityEstimates|^func sqlSetExplainCardinalityEstimate' hat/hatSql/query.go || true
  exit 0
fi

if [[ "${1:-all}" == "query-struct" ]]; then
  rg -n -A 70 -B 8 '^type sqlQuery struct' hat/hatSql/query.go
  exit 0
fi

if [[ "${1:-all}" == "benchmark-tail" ]]; then
  tail -n 40 BENCHMARK.md
  exit 0
fi

if [[ "${1:-all}" == "adopted-mz044" ]]; then
  rg -n -A 3 -B 3 'MZ-044|Costed dataflow|arrangement cost' ADOPTED_QUERY_ENGINE_IDEAS.md || true
  exit 0
fi

if [[ "${1:-all}" == "adopted-head" ]]; then
  sed -n '1,35p' ADOPTED_QUERY_ENGINE_IDEAS.md
  exit 0
fi

printf '%s\n' '=== Existing proposal, gap, and benchmark documents ==='
rg --files -g '*.md' | rg -i '(^|/)(README|.*(IDEA|PROPOSAL|BENCHMARK|COMMAND|PRODUCT|DATA_STRUCTURE)).*\.md$' | sort || true

printf '%s\n' '=== Existing inspiration references and status markers ==='
rg -n -i -m 240 'clickhouse|materialize|tarantool|CH-[A-Z0-9]+|TR-[A-Z0-9]+|MZ-[A-Z0-9]+|adopted|implemented|backlog|unchecked|TODO' --glob '*.md' --glob '*.go' --glob '*.sh' --glob 'Makefile' . || true

printf '%s\n' '=== Candidate concepts: current implementation hits ==='
for concept in \
  'materialized view' \
  'projection' \
  'merge tree' \
  'skip index' \
  'bloom' \
  'sampling' \
  'ttl' \
  'time travel' \
  'savepoint' \
  'watermark' \
  'snapshot' \
  'compaction' \
  'queue' \
  'changefeed' \
  'CDC' \
  'schema evolution' \
  'tiered' \
  'histogram' \
  'approx_count_distinct' \
  'asof join' \
  'with ties' \
  'index hint' \
  'prepared'; do
  printf '\n-- %s --\n' "$concept"
  rg -n -i -m 24 --glob '*.md' --glob '*.go' --glob '*.sh' --glob 'Makefile' "$concept" . || true
done

printf '%s\n' '=== Recent commits relevant to inspiration work ==='
git log -20 --oneline --decorate --all -- '*.md' 'hat' 'scripts' 'Makefile' || true
