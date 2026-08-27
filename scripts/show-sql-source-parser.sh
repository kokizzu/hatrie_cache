#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

rg -n -A 220 'func \(p \*sqlQueryParser\) parse.*Source' "$root/hat/hatSql/query.go"
