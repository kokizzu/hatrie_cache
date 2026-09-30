#!/usr/bin/env bash
set -euo pipefail

clickhouse_count=$(rg -c --no-filename 'CH-[0-9][0-9] [|]' ENGINE_INSPIRATION_GAPS.md)
materialize_count=$(rg -c --no-filename 'MZ-[0-9][0-9] [|]' ENGINE_INSPIRATION_GAPS.md)
tarantool_count=$(rg -c --no-filename 'TT-[0-9][0-9] [|]' ENGINE_INSPIRATION_GAPS.md)
test "$clickhouse_count" -eq 50
test "$materialize_count" -eq 50
test "$tarantool_count" -eq 50
rg -n 'ENGINE_INSPIRATION_GAPS.md' README.md
printf 'catalog counts: ClickHouse=%s, Materialize=%s, Tarantool=%s\n' "$clickhouse_count" "$materialize_count" "$tarantool_count"
