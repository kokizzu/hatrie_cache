#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --check
git diff --stat
git diff -- Makefile README.md BENCHMARK.md INSPIRATION_BACKLOG.md CH022_EXPLAIN_PRUNING.md hat/hatSql/model.go hat/hatSql/query.go hat/hatSql/ch022_explain_pruning_test.go hat/hatSql/ch022_explain_pruning_benchmark_test.go scripts/test-ch022-explain-pruning.sh scripts/benchmark-ch022-explain-pruning.sh scripts/format-ch022.sh scripts/test-ch022-race.sh scripts/test-ch022-full.sh
