#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'Frontier read-hold implementation:'
sed -n '1,280p' hat/hatDataStructure/frontier_read_hold.go
printf '%s\n' '' 'TypedTable changefeed implementation:'
sed -n '1,220p' hat/hatSql/typed_table_changefeed.go 2>/dev/null || true
rg -n -C 5 'ChangesAfter|CompactChangesThrough|compactedThrough|sequence' hat/hatSql/typed_table.go hat/hatSql --glob '*.go'
