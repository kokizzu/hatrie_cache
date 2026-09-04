#!/bin/sh
set -eu

printf '%s\n' '== inspiration status =='
printf '%s\n' 'sample entries:'
rg -n 'C001|C073|M001|T001' INSPIRATION.md | head -n 12 || true
for marker in x ' ' - '>'; do
  count="$(rg -c "^[[:space:]]*-[[:space:]]+\\[$marker\\][[:space:]]+[CMT][0-9]+" INSPIRATION.md || true)"
  printf 'marker [%s]: %s\n' "$marker" "$count"
done
printf '%s\n' '== documentation anchors =='
rg -n 'INSPIRATION|ADOPTED_QUERY|SQL_TWO|SQL_VECTORIZED|vectorized|two-level|Two-Level' README.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md | head -n 180 || true
printf '%s\n' 'unresolved examples:'
rg -n '^[[:space:]]*-[[:space:]]+\[[ -]\][[:space:]]+[CMT][0-9]+' INSPIRATION.md | head -n 25 || true

printf '%s\n' '== expression evaluation symbols =='
rg -n 'type sqlExpr|func evalSQLExpr|case "(function|case|in|between|is|unary|binary)"|func .*Function' hat/hatSql/*.go | head -n 220 || true
printf '%s\n' '== parser/rewrite entry points =='
rg -n 'func (parse|bind|rewrite)SQL|parseSQLQuery|rewriteSQLQuery' hat/hatSql/*.go | head -n 180 || true
printf '%s\n' '== query execution entry points =='
rg -n 'func (ExecuteSQLQuery|ExecuteSQLQueryContext|executeSQL.*Query|executeSQL.*Rows)' hat/hatSql/query.go | head -n 180 || true
printf '%s\n' '== evaluator excerpt =='
sed -n '14340,14630p' hat/hatSql/query.go
printf '%s\n' '== execution panic boundary =='
sed -n '9550,9610p' hat/hatSql/query.go
printf '%s\n' '== execution control construction =='
rg -n 'type sqlExecutionControl|newSQLExecutionControl|sqlExecutionControl\{' hat/hatSql/*.go | head -n 180 || true
sed -n '6900,7010p' hat/hatSql/query.go

printf '%s\n' '== worktree =='
git status --short
