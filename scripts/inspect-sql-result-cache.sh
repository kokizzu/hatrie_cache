#!/usr/bin/env sh
set -eu

sed -n '1,280p' hat/hatSql/result_cache_test.go
sed -n '1,140p' hat/hatSql/model.go
