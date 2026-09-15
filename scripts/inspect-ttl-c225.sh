#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'TTL implementation files:'
rg --files hat/hatSql | rg 'ttl|typed_table'
printf '%s\n' '' 'TTL symbols and call sites:'
rg -n 'TypedTableTTL|ttl|deadline|Expire' hat/hatSql --glob '*.go'
printf '%s\n' '' 'typed_table_ttl.go:'
sed -n '1,240p' hat/hatSql/typed_table_ttl.go
printf '%s\n' '' 'ch007_row_ttl_benchmark_test.go:'
sed -n '1,180p' hat/hatSql/ch007_row_ttl_benchmark_test.go
printf '%s\n' '' 'typed_table_patch_parts.go:'
sed -n '1,190p' hat/hatSql/typed_table_patch_parts.go
printf '%s\n' '' 'run-ch007-ttl-c225.sh:'
sed -n '1,100p' scripts/run-ch007-ttl-c225.sh
printf '%s\n' '' 'typed_table.go mutation paths:'
sed -n '450,590p' hat/hatSql/typed_table.go
