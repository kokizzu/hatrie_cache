#!/usr/bin/env sh
set -eu

printf '%s\n' '=== public execution and storage APIs ==='
grep -R -n -E '^(type|func) [A-Z].*(Columnar|Adaptive|Dictionary|Arena|Join|Cache|Warm|Lock|Shard|Decode|Merge)' hat/hatSql hat/hatCache --include='*.go' || true

printf '%s\n' '=== columnar cache integration ==='
sed -n '1,280p' hat/hatCache/sql.go
sed -n '1,220p' hat/hatCache/sql_columnar_scan_test.go
sed -n '1,180p' hat/hatCache/sql_columnar_scan_benchmark_test.go

printf '%s\n' '=== adaptive and join planner ==='
sed -n '1,240p' hat/hatSql/adaptive.go
sed -n '1,360p' hat/hatSql/join_plan_test.go
