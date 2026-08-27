#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
sed -n '645,735p' "$root/hat/hatSql/query.go"
