#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatSql/adaptive.go hat/hatSql/adaptive_concurrency_test.go hat/hatSql/adaptive_concurrency_benchmark_test.go scripts/inspect-sql-adaptive-storage.sh scripts/format-sql-adaptive-concurrency.sh scripts/test-sql-adaptive-concurrency.sh scripts/benchmark-sql-adaptive-concurrency.sh scripts/review-sql-adaptive-concurrency.sh scripts/commit-sql-adaptive-concurrency.sh
git diff -- Makefile
go test ./hat/hatSql -run '^TestAdaptivePlannerConcurrentPredicateFeedback$' -count=1
go test -race ./hat/hatSql -run '^TestAdaptivePlannerConcurrentPredicateFeedback$' -count=1
go test ./hat/hatSql -run '^$' -bench '^BenchmarkAdaptivePlannerConcurrentFeedback$' -benchmem -count=5
go test ./...
