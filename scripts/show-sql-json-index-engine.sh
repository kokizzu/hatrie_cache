#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

printf '%s\n' '== JSON index API =='
rg -n -A 120 -B 12 'func \(.*\) CreateSQLJSON(Field|Text|Composite)Index' "$root/hat/hatCache/sql_query.go"
rg -n -A 100 -B 16 'CreateSQLJSONFieldIndex' "$root/hat/hatCache"
rg -n -A 160 -B 20 'func refreshSQLJSON(Field|Text|Composite)Index' "$root/hat/hatCache/sql_query.go"
printf '%s\n' '== SQL expression evaluation =='
rg -n -A 180 -B 20 'func evalSQLExpr' "$root/hat/hatSql/query.go"
printf '%s\n' '== indexed predicate planning =='
sed -n '7988,8038p' "$root/hat/hatSql/query.go"
rg -n -A 180 -B 20 'func resolveSQLIndexedComparison' "$root/hat/hatSql/query.go"
rg -n -A 140 -B 24 'resolveSQLIndexedComparison\(' "$root/hat/hatSql/query.go"
