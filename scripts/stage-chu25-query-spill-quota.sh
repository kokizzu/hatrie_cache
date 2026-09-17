#!/bin/sh
set -eu

git add -- scripts/commit-chu25-query-spill-quota.sh scripts/push-chu25-query-spill-quota.sh

git add -- CHU25_QUERY_SPILL_QUOTA.md BENCHMARK.md Makefile PRODUCT_IDEA_GAPS.md README.md hat/hatSql/query.go hat/hatSql/spill_quota.go hat/hatSql/chu25_query_spill_quota_test.go hat/hatSql/chu25_query_spill_quota_internal_test.go hat/hatSql/chu25_query_spill_quota_benchmark_test.go scripts/benchmark-chu25-query-spill-quota.sh scripts/format-chu25-query-spill-quota.sh scripts/race-chu25-query-spill-quota.sh scripts/review-chu25-query-spill-quota.sh scripts/stage-chu25-query-spill-quota.sh scripts/test-chu25-query-spill-quota.sh scripts/vet-chu25-query-spill-quota.sh
git diff --cached --check
git diff --cached --stat
