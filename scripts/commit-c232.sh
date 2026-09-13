#!/usr/bin/env bash
set -euo pipefail

case "${1:-commit}" in
status)
    git status --short
    git branch --show-current
    git log -1 --oneline
    ;;
commit)
    git add Makefile README.md BENCHMARK.md INSPIRATION_ROUND2.md C232_INTERMEDIATE_ROWS.md \
        hat/hatSql/query.go hat/hatSql/sql_result_cache.go hat/hatSql/governance.go \
        hat/hatSql/intermediate_rows_test.go \
        hat/hatSql/intermediate_rows_baseline_benchmark_test.go \
        hat/hatSql/intermediate_rows_guard_benchmark_test.go \
        scripts/test-c232.sh scripts/commit-c232.sh scripts/push-c232.sh
    git commit -m 'feat(hatSql): add intermediate row limits'
    ;;
amend)
    git add Makefile scripts/commit-c232.sh scripts/push-c232.sh
    git commit --amend --no-edit
    ;;
*)
    printf 'unknown c232 git mode: %s\n' "$1" >&2
    exit 2
    ;;
esac
