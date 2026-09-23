#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'Scoped source audit:'
sed -n '1,240p' CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md

printf '\n%s\n' 'Open adoption catalog:'
sed -n '1,260p' IDEA_GAP_CATALOG.md

printf '\n%s\n' 'Ledger headings/statuses:'
for file in INSPIRATION.md INSPIRATION_BACKLOG.md INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md PRODUCT_IDEA_GAPS.md; do
  if [[ -f "$file" ]]; then
    printf '\n--- %s ---\n' "$file"
    rg -n '^(#|##|###)|^- \[[ xX-\]' "$file" || true
  fi
done

printf '\n%s\n' 'Recent numbered feature files under hat/:'
rg --files hat | rg '/m2(0[0-9]|1[0-9]|2[0-9]|3[0-9]|4[0-9])_' | sort

printf '\n%s\n' 'Replay implementation/test files:'
rg -l 'ReplayWithProgress|Replay' hat scripts | rg '(replay|journal|outbox)' | sort

printf '\n%s\n' 'Current feature references:'
rg -n 'M22[0-9]|MZ-0[9]|CH-U[0-9]|Tarantool|Materialize|ClickHouse' README.md INSPIRATION_ROUND2.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md || true
