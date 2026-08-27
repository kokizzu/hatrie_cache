#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

printf '%s\n' '== native roaring bitmap API =='
sed -n '1,80p' "$root/hat/hatCache/sql_query.go"
rg -n -A 40 -B 4 'func \(bitmap \*?RoaringBitmap\) (Add|Contains|Cardinality|Values|Each|And|Or|Info)' "$root/hat/hatDataStructure/roaring.go"
printf '%s\n' '== SQL index state =='
rg -n -A 100 -B 16 '^type HatTrie struct' "$root/hat/hatCache"
rg -n -A 100 -B 20 'sqlJSONIndexes|sqlIndexMu' "$root/hat/hatCache/sql_query.go"
