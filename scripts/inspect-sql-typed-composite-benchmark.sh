#!/bin/sh
set -eu

sed -n '1,50p' hat/hatCache/main_test.go
sed -n '1,180p' hat/hatCache/sql_typed_index_baseline_benchmark_test.go
sed -n '1460,1785p' hat/hatCache/sql_query.go
