#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md INSPIRATION_BACKLOG.md CH022_EXPLAIN_PRUNING.md hat/hatSql/model.go hat/hatSql/query.go hat/hatSql/ch022_explain_pruning_test.go hat/hatSql/ch022_explain_pruning_benchmark_test.go scripts/test-ch022-explain-pruning.sh scripts/benchmark-ch022-explain-pruning.sh scripts/format-ch022.sh scripts/test-ch022-race.sh scripts/test-ch022-full.sh scripts/review-ch022.sh scripts/commit-ch022.sh scripts/push-ch022.sh
git diff --cached --check
git diff --cached --stat
git commit -m 'feat: add structured explain pruning telemetry'
