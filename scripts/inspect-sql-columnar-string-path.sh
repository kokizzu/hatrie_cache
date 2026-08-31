#!/bin/sh
set -eu

sed -n '7210,7300p' hat/hatSql/query.go
sed -n '7935,8025p' hat/hatSql/query.go
sed -n '368,485p' hat/hatCache/sql_columnar_layout_cache.go
