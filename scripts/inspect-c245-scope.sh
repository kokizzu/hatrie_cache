#!/usr/bin/env bash
set -euo pipefail

rg -n -C 2 'C245|vertical TTL|deletion mask|delete bitmap' \
  INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md README.md BENCHMARK.md
sed -n '1,300p' hat/hatDataStructure/persistent_delete_bitmap.go
sed -n '1,260p' hat/hatDataStructure/vertical_ttl_delete_benchmark_test.go
sed -n '1,300p' hat/hatSql/typed_table_delete_bitmap.go
sed -n '1,280p' hat/hatSql/typed_table_ttl.go
sed -n '1,260p' hat/hatSql/typed_table_ttl_scheduler.go
rg -n -A 140 '^func \(table \*TypedTable\) PurgeExpired' hat/hatSql/typed_table_ttl.go
rg -n -A 100 'type TypedTable struct|func \(table \*TypedTable\) Keys|func \(table \*TypedTable\) Key' hat/hatSql/typed_table.go
rg -n 'C245|vertical.?ttl|deletion.?mask|delete.?bitmap|expired.*row' \
  Makefile scripts --glob '*.sh' --glob 'Makefile'
