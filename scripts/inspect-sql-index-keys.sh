#!/usr/bin/env sh
set -eu

grep -n -A 100 -B 40 '^func sqlIndexValueKey' hat/hatCache/sql_query.go
grep -R -n -m 240 -E 'sqlIndexValueKey|SQLJSON.*Index|CreateSQL.*Index' hat/hatCache/*_test.go
