#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

printf '== SQL mutation tests ==\n'
sed -n '1,520p' "$root/hat/hatCache/sql_test.go"
