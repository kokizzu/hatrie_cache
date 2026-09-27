#!/usr/bin/env bash
set -euo pipefail

file="ENGINE_INSPIRATION_150.md"
test -f "$file"

for prefix in CHG MZG TTG; do
  count="$(rg -c "^\\| ${prefix}-[0-9]{3} \\|" "$file")"
  test "$count" -eq 50
done

test "$(rg -c '^## ClickHouse: 50 Gaps$' "$file")" -eq 1
test "$(rg -c '^## Materialize: 50 Gaps$' "$file")" -eq 1
test "$(rg -c '^## Tarantool: 50 Gaps$' "$file")" -eq 1
printf 'validated 150 engine inspiration gaps\n'
