#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
rg -n -A 18 '^type sqlSource struct' "$root/hat/hatSql/query.go"
rg -n -A 35 -B 4 'keyParameter' "$root/hat/hatSql/query.go"
rg -n -A 42 '^func sqlClauseKeyword' "$root/hat/hatSql/query.go"
rg -n -A 24 -B 12 'sqlTokenParameter' "$root/hat/hatSql/query.go"
sed -n '7690,7780p' "$root/hat/hatSql/query.go"
