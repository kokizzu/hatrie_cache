#!/usr/bin/env sh
set -eu

rg -n -A 220 -B 20 '^type AdaptivePlannerOptions|^func .*Adaptive' hat/hatSql/adaptive.go
rg -n -A 180 -B 20 '^func TestSQLAdaptive|^func BenchmarkSQLAdaptive' hat/hatCache/sql_adaptive_planner_test.go
rg -n -m 200 'AdaptivePlanner|Observe\(|PreferColumnar|Columnar' hat/hatSql hat/hatCache/sql_*.go
rg -n -m 120 'planner\.stats|planner\.mu|adaptivePlannerStats' hat/hatSql hat/hatCache
