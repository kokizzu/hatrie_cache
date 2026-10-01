#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add BENCHMARK.md Makefile PRODUCT_IDEA_GAPS.md README.md T_U09_JOIN_BOOTSTRAP.md hat/hatReplication/join_bootstrap.go hat/hatReplication/join_bootstrap_benchmark_test.go hat/hatReplication/join_bootstrap_test.go scripts/benchmark-t-u09.sh scripts/format-t-u09.sh scripts/test-t-u09.sh scripts/verify-t-u09-focused.sh scripts/verify-t-u09.sh scripts/deliver-t-u09.sh
git diff --cached --check
git diff --cached --stat
git commit -m 'feat(replication): gate snapshot WAL joins [skip ci]'
git push origin HEAD
git status --short
