#!/bin/sh
set -eu

sed -n '7518,7565p' hat/hatSql/query.go
sed -n '7938,7972p' hat/hatSql/query.go
