#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
rg -n -A 80 -B 8 '^func sqlBuiltinFunction|unknown SQL function|SQLFunctionResolver' "$root/hat/hatSql/query.go"
