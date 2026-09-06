#!/bin/sh
set -eu

printf '%s\n' 'Explain result contracts:'
sed -n '15,65p' hat/hatSql/model.go
printf '%s\n' 'Explain plan construction:'
sed -n '11011,11074p' hat/hatSql/query.go
printf '%s\n' 'Optimizer rejected-alternative details:'
sed -n '12020,12180p' hat/hatSql/query.go
printf '%s\n' 'Explain tests and compatibility assertions:'
sed -n '1,180p' hat/hatSql/join_plan_test.go
printf '%s\n' 'Optimizer candidate call sites:'
rg -n -C 12 'resolveSQLMostSelectiveIndexedConjunct|INDEX CANDIDATES' hat/hatSql/query.go
printf '%s\n' 'Checklist and README documentation anchors:'
rg -n -C 5 'M080|Explain optimizer|INSPIRATION|Documentation|EXPLAIN' INSPIRATION.md README.md
