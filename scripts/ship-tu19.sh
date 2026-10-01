#!/usr/bin/env bash
set -euo pipefail

files=(
    ADOPTED_QUERY_ENGINE_IDEAS.md
    BENCHMARK.md
    PRODUCT_IDEA_GAPS.md
    Makefile
    TU19_DURABLE_TUPLE_FIELD_JOURNAL.md
    hat/hatDataStructure/tu19_tuple_field_update_journal.go
    hat/hatDataStructure/tu19_tuple_field_update_journal_test.go
    hat/hatDataStructure/tu19_tuple_field_update_journal_benchmark_test.go
    scripts/run-tu19-tuple-journal.sh
    scripts/verify-tu19-ship.sh
    scripts/ship-tu19.sh
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
    git commit -m "feat(data): add durable tuple field journal records [skip ci]"
    ;;
push)
    git push origin HEAD
    ;;
*)
    printf 'usage: %s {review|commit|push}\n' "$0" >&2
    exit 2
    ;;
esac
