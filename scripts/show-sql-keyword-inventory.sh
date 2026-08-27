#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

sed -n '280,390p' "$root/hat/hatCache/sql_function_test.go"
