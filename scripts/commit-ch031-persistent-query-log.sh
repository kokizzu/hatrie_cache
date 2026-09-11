#!/usr/bin/env bash
set -euo pipefail

git add ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md ENGINE_IDEAS.md Makefile PERSISTENT_QUERY_LOG.md README.md hat/hatCache/sql_query.go hat/hatSql/query_log.go hat/hatSql/query_log_benchmark_test.go hat/hatSql/query_log_test.go hat/hatSql/query_manager.go hat/hatSql/query_manager_execute_benchmark_test.go scripts/benchmark-ch031-baseline.sh scripts/benchmark-ch031-persistent-query-log.sh scripts/commit-ch031-persistent-query-log.sh scripts/format-ch031-persistent-query-log.sh scripts/push-ch031-persistent-query-log.sh scripts/review-ch031-persistent-query-log.sh scripts/test-ch031-persistent-query-log.sh scripts/test-race-ch031-persistent-query-log.sh scripts/verify-ch031-docs.sh scripts/vet-ch031-persistent-query-log.sh sql_query_manager_api.go
git commit -m 'hatSql: add persistent query log'
