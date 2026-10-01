#!/usr/bin/env bash
set -euo pipefail

files=(
    ADOPTED_QUERY_ENGINE_IDEAS.md
    BENCHMARK.md
    PRODUCT_IDEA_GAPS.md
    Makefile
    TU05_SESSION_TRANSACTION_SETTINGS.md
    hat/hatSql/tu05_session_transaction_settings.go
    hat/hatSql/tu05_session_transaction_settings_test.go
    hat/hatSql/tu05_session_transaction_settings_baseline_benchmark_test.go
    hat/hatSql/tu05_session_transaction_settings_benchmark_test.go
    scripts/run-tu05-session-settings.sh
    scripts/verify-tu05-ship.sh
    scripts/ship-tu05.sh
)

case "${1:-review}" in
review)
    git diff --check -- "${files[@]}"
    git diff --stat -- "${files[@]}"
    git status --short --untracked-files=all
    ;;
commit)
    git add -- "${files[@]}"
    git diff --cached --check
    git commit -m "feat(sql): add session transaction settings [skip ci]"
    ;;
push)
    git push origin HEAD
    ;;
*)
    printf 'usage: %s {review|commit|push}\n' "$0" >&2
    exit 2
    ;;
esac
