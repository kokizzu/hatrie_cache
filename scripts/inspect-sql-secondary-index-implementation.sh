#!/usr/bin/env sh
set -eu

sed -n '560,870p' hat/hatCache/sql_query.go
sed -n '950,1265p' hat/hatCache/sql_query.go
sed -n '1545,1730p' hat/hatCache/sql_query.go
sed -n '1,260p' hat/hatCache/sql_typed_index_baseline_benchmark_test.go
