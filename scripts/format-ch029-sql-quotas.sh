#!/bin/sh
set -eu

gofmt -w hat/hatSql/quota.go hat/hatSql/query.go hat/hatSql/ch029_sql_quota_test.go hat/hatSql/ch029_sql_quota_benchmark_test.go hat/hatCache/sql_quota.go
