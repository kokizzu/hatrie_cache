#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

printf '== SQL mutation compiler ==\n'
sed -n '1,680p' "$root/hat/hatCache/sql.go"

printf '\n== SQL transaction implementation ==\n'
sed -n '1,190p' "$root/hat/hatCache/sql_transaction.go"
