#!/bin/sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatSql/spill_quota.go hat/hatSql/chu25_query_spill_quota_test.go hat/hatSql/chu25_query_spill_quota_internal_test.go hat/hatSql/chu25_query_spill_quota_benchmark_test.go
