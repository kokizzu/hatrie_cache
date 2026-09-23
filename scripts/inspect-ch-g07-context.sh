#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '== grouping syntax and metadata =='
rg -n "groupingSets|groupingDimensions|GROUPING SETS|ROLLUP|CUBE|GROUPING\(" hat/hatSql --glob '*.go'
printf '%s\n' '== grouped execution entry points =='
rg -n "func .*Group|func .*group|groupBy|groupingSets|groupingDimensions" hat/hatSql --glob '*.go'
printf '%s\n' '== grouping-set implementation =='
sed -n '1,240p' hat/hatSql/grouping_sets.go
printf '%s\n' '== grouping-set tests =='
sed -n '1,220p' hat/hatSql/grouping_sets_test.go
printf '%s\n' '== grouping-identifier tests =='
sed -n '1,230p' hat/hatSql/grouping_identifier_test.go
