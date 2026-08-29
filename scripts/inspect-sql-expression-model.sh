#!/usr/bin/env sh
set -eu

sed -n '4560,4625p' hat/hatSql/query.go
sed -n '11120,11210p' hat/hatSql/query.go
sed -n '11840,11980p' hat/hatSql/query.go
