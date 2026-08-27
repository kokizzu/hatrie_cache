#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

printf '== hatSql declarations ==\n'
rg -n '^(type|func) ' \
    "$root/hat/hatSql/query.go" \
    "$root/hat/hatSql/contracts.go" \
    "$root/hat/hatSql/error.go" \
    "$root/hat/hatSql/collation.go" \
    "$root/hat/hatSql/snapshot.go" \
    "$root/hat/hatSql/adaptive.go"

printf '\n== hatCache SQL declarations ==\n'
rg -n '^(type|func) ' \
    "$root/hat/hatCache/sql.go" \
    "$root/hat/hatCache/sql_query.go" \
    "$root/hat/hatCache/sql_transaction.go" \
    "$root/hat/hatCache/sql_function.go" \
    "$root/hat/hatCache/sql_replica.go"

printf '\n== Requested feature symbols ==\n'
rg -n -i \
    'parse.*merge|merge.*statement|upsert|returning|savepoint|tablefunction|table function|jsonpath|json_path|json_extract|bitmapindex|bitmap index|approx_count_distinct|approx.*percentile|tablesample|sample clause|planguard|plan guard|opentelemetry|prometheus' \
    "$root/hat/hatSql" \
    "$root/hat/hatCache" \
    "$root/cmd" \
    --glob '*.go'
