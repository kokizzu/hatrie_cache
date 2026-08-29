#!/usr/bin/env sh
set -eu

sed -n '1,220p' hat/hatSql/result_cache.go
sed -n '1,180p' hat/hatSql/cache_warming.go
sed -n '4050,4170p' hat/hatSql/query.go
