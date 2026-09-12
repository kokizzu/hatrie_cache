#!/bin/sh
set -eu

git add BENCHMARK.md CH028_MAX_THREADS.md ENGINE_IDEAS.md README.md Makefile hat/hatSql/query.go hat/hatSql/ch028_max_threads_test.go hat/hatSql/ch028_max_threads_benchmark_test.go hat/hatCache/sql_query.go scripts/benchmark-ch028-max-threads.sh scripts/commit-ch028-max-threads.sh scripts/format-ch028-max-threads.sh scripts/push-ch028-max-threads.sh scripts/race-ch028-max-threads.sh scripts/review-ch028-max-threads.sh scripts/test-ch028-max-threads.sh scripts/verify-ch028-docs.sh scripts/vet-ch028-max-threads.sh
git commit -m 'feat: add bounded SQL max_threads setting'
