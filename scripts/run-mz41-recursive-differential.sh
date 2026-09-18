#!/usr/bin/env bash
set -eu

case "${1:-}" in
    test)
        go test ./hat/hatSql -run 'TestMutableRecursiveReachability'
        ;;
    race)
        go test -race ./hat/hatSql -run 'TestMutableRecursiveReachability'
        ;;
    benchmark)
        go test ./hat/hatSql -run '^$' -bench 'BenchmarkM064|BenchmarkMutableRecursiveReachability' -benchmem -count=5
        ;;
    vet)
        go vet ./hat/hatSql
        ;;
    review)
        git diff --check
        git diff --cached --check
        git status --short
        git diff -- INSPIRATION_BACKLOG.md Makefile scripts/run-mz41-recursive-differential.sh
        ;;
    stage)
        git add INSPIRATION_BACKLOG.md scripts/run-mz41-recursive-differential.sh
        git apply --cached --check /tmp/mz41-makefile-stage.patch
        git apply --cached /tmp/mz41-makefile-stage.patch
        rm -f /tmp/mz41-makefile-stage.patch
        ;;
    commit)
        git commit -m "docs: close MZ-41 recursive differential gap [skip ci]"
        ;;
    push)
        git push origin HEAD:master
        ;;
    status)
        git status --short --branch
        ;;
    *)
        printf '%s\n' 'usage: run-mz41-recursive-differential.sh {test|race|benchmark|vet|review|stage|commit|push|status}' >&2
        exit 2
        ;;
esac
