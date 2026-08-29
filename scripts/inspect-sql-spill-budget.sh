#!/usr/bin/env sh
set -eu

grep -n -A 180 -B 30 '^type sqlSpillEncoder' hat/hatSql/query.go
grep -n -A 180 -B 30 '^func newSQLSpillEncoder' hat/hatSql/query.go
grep -n -A 140 -B 20 '^func Test.*External.*Sort\|^func Test.*Spill.*Budget' hat/hatSql/*_test.go
