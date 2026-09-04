#!/bin/sh
set -eu

file=INSPIRATION.md
test -f "$file"

count="$(rg -o '^- \[[x >-]\] [CMT][0-9]{3} ' "$file" | awk 'END { print NR + 0 }')"
test "$count" -ge 300
rg -q 'C019 Version-aware bounded query-result cache' "$file"
rg -q 'M005 Shared arrangements' "$file"
rg -q 'T048 Replication sets' "$file"
rg -q 'ClickHouse query optimization guide' "$file"
printf 'inspiration checklist: %s items\n' "$count"
