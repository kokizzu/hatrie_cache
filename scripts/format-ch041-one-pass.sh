#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/ch041_one_pass_grouping.go hat/hatSql/ch041_one_pass_grouping_test.go hat/hatSql/ch041_one_pass_grouping_benchmark_test.go hat/hatSql/grouping_sets.go hat/hatSql/grouping_sets_test.go hat/hatSql/query.go
