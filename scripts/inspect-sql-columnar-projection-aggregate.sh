#!/bin/sh
set -eu

rg -n -A 220 -B 20 '^func executeSQLColumnarNumericAggregate|^func sqlColumnarNumericAggregates|^func sqlIndexedGroup|^func executeSQLIndexedGroup|^func sqlOrderedGroupProjections|^func sqlCoveringProjectionFields' hat/hatSql/query.go
rg -n -A 40 -B 10 'SQLCollation(Binary|NoCase)|type SQLCollation|func sqlQueryCollation' hat/hatSql
rg -n 'CoveringIndexedSourceResolver|ResolveSQLCoveringSource|executeSQLColumnarNumericAggregate|COLUMNAR NUMERIC AGGREGATE|IndexedGroup' hat/hatSql/*_test.go || true
