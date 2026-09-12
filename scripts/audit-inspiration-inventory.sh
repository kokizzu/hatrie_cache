#!/bin/sh
set -eu

printf '%s\n' '== checklist counts =='
awk '
/^[-*] \[[ x-]\]/ {
  if ($0 ~ /C[0-9][0-9][0-9]/) c++
  if ($0 ~ /M[0-9][0-9][0-9]/) m++
  if ($0 ~ /T[0-9][0-9][0-9]/) t++
  if ($0 ~ /\[x\]/) done++
  if ($0 ~ /\[ \]/) open++
  if ($0 ~ /\[-\]/) deferred++
}
END {
  printf "C entries: %d\nM entries: %d\nT entries: %d\nDone: %d\nOpen: %d\nDeferred: %d\n", c, m, t, done, open, deferred
}' INSPIRATION.md

printf '%s\n' '== current entries =='
rg -n '^[-*] \[[ x-]\] (C|M|T)[0-9][0-9][0-9]' INSPIRATION.md

printf '%s\n' '== new backlog counts =='
awk '
/^## ClickHouse/ { section = "clickhouse"; next }
/^## Materialize/ { section = "materialize"; next }
/^## Tarantool/ { section = "tarantool"; next }
/^\| CH-[0-9][0-9] \|/ { if (section == "clickhouse") clickhouse++ }
/^\| MZ-[0-9][0-9] \|/ { if (section == "materialize") materialize++ }
/^\| TR-[0-9][0-9] \|/ { if (section == "tarantool") tarantool++ }
END {
  printf "ClickHouse candidates: %d\nMaterialize candidates: %d\nTarantool candidates: %d\n", clickhouse, materialize, tarantool
  if (clickhouse != 50 || materialize != 50 || tarantool != 50) exit 1
}' INSPIRATION_BACKLOG.md
