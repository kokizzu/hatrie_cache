#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

rg -n -A 260 'func (resolveSQLSourceRows|streamSQLSourceRows)' "$root/hat/hatSql/query.go"

printf '\n== materialized external source handling ==\n'
rg -n -A 140 'case "EXTERNAL"' "$root/hat/hatSql/query.go"
