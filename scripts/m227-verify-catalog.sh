#!/usr/bin/env bash
set -euo pipefail

catalog=IDEA_GAP_CATALOG.md
for prefix in CH MZ TT; do
  count="$(rg -c "^\| ${prefix}-G[0-9]{2} \|" "$catalog")"
  if [[ "$count" != "50" ]]; then
    printf 'catalog prefix %s count=%s, want 50\n' "$prefix" "$count" >&2
    exit 1
  fi
done

rg -q '^\| TT-G08 \|' "$catalog"
rg -q 'ClickHouse query optimization' "$catalog"
rg -q 'Materialize concepts and arrangements' "$catalog"
rg -q 'Tarantool.*platform, transactions, WAL, replication, and indexes' "$catalog"
printf '%s\n' 'IDEA_GAP_CATALOG.md: 50 ClickHouse + 50 Materialize + 50 Tarantool candidates verified.'
