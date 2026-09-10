#!/usr/bin/env bash
set -eu

git add ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md INCREMENTAL_RECURSIVE_REACHABILITY.md INSPIRATION.md Makefile README.md hat/hatSql/mutable_recursive_reachability.go hat/hatSql/m064_mutable_recursive_reachability_test.go hat/hatSql/recursive_reachability.go hat/hatSql/recursive_reachability_benchmark_test.go scripts/benchmark-m064-mutable-recursive-reachability.sh scripts/commit-m064-mutable-recursive-reachability.sh scripts/format-m064-mutable-recursive-reachability.sh scripts/push-m064-mutable-recursive-reachability.sh scripts/review-m064-mutable-recursive-reachability.sh scripts/test-m064-mutable-recursive-reachability.sh scripts/test-m064-reachability-all.sh scripts/test-race-m064-mutable-recursive-reachability.sh scripts/vet-m064-mutable-recursive-reachability.sh
git diff --cached --check
git commit -m 'feat(sql): add mutable recursive reachability maintenance'
