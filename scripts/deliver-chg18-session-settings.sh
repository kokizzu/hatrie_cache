#!/usr/bin/env bash
set -euo pipefail

files=(
    Makefile
    PRODUCT_IDEA_GAPS.md
    TU05_TRANSACTION_SESSION_SETTINGS.md
    hat/hatCache/sql_transaction_session.go
    hat/hatCache/sql_transaction_session_test.go
    hat/hatCache/sql_transaction_session_benchmark_test.go
    scripts/chg18-session-settings.sh
    scripts/deliver-chg18-session-settings.sh
)

mode="${1:-}"
case "${mode}" in
    status)
        git status --short -- "${files[@]}"
        git diff --check -- "${files[@]}"
        git diff --stat -- "${files[@]}"
        ;;
    stage)
        git add -- "${files[@]}"
        git diff --cached --check -- "${files[@]}"
        git status --short -- "${files[@]}"
        ;;
    commit)
        git diff --cached --quiet -- "${files[@]}" && {
            printf 'no staged session-settings changes\n' >&2
            exit 1
        }
        git commit -m 'feat(sql): add transaction session defaults [skip ci]'
        ;;
    push)
        git push -u origin HEAD
        ;;
    *)
        printf 'usage: %s status|stage|commit|push\n' "$0" >&2
        exit 2
        ;;
esac
