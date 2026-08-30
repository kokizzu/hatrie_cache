#!/bin/sh
set -eu

sed -n '7518,7565p' hat/hatSql/query.go
sed -n '7938,7972p' hat/hatSql/query.go
sed -n '1510,1600p' hat/hatSql/query.go
rg -n -C 3 'sqlQueryRowsBaseStreamable' hat/hatSql/query.go
sed -n '728,930p' hat/hatSql/query.go
sed -n '660,727p' hat/hatSql/query.go
rg -n -C 4 'type sqlQueryObservation|func \(.*sqlQueryObservation.*resultBytes|resultBytes' hat/hatSql --glob '*.go'
