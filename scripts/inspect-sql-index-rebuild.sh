#!/usr/bin/env sh
set -eu

sed -n '9450,9497p' hat/hatSql/query.go
sed -n '9551,9594p' hat/hatSql/query.go
