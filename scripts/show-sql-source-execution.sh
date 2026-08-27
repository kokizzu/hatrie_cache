#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

rg -n -A 260 'func (resolveSQLSourceRows|streamSQLSourceRows)' "$root/hat/hatSql/query.go"
