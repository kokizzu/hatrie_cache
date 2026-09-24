#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/grouping_sets.go hat/hatSql/ch041_grouping_branch_plan_test.go hat/hatSql/ch041_grouping_query_benchmark_test.go
