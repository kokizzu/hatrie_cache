#!/bin/sh
set -eu

sed -n '130,225p' hat/hatSql/query.go
rg -n -C 5 'IndexAdvisor|SlowQueryThreshold' hat/hatSql/query.go
sed -n '1,260p' hat/hatSql/index_advisor.go
sed -n '1,280p' hat/hatSql/materialized.go
