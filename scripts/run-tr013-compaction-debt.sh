#!/usr/bin/env bash
set -eu

case "${1:-}" in
    format)
        gofmt -w hat/hatCache/compaction_debt_scheduler.go hat/hatCache/tr013_compaction_debt_scheduler_test.go hat/hatCache/tr013_compaction_debt_scheduler_benchmark_test.go
        ;;
    test)
        go test ./hat/hatCache -run 'TestTR013CompactionDebtScheduler'
        ;;
    race)
        go test -race ./hat/hatCache -run 'TestTR013CompactionDebtScheduler'
        ;;
    benchmark)
        go test ./hat/hatCache -run '^$' -bench 'BenchmarkTR013' -benchmem -count=5
        ;;
    vet)
        go vet ./hat/hatCache
        ;;
    package)
        go test ./hat/hatCache
        ;;
    review)
        git diff --check
        git diff --cached --check
        git status --short
        git diff -- BENCHMARK.md INSPIRATION_BACKLOG.md README.md TR013_COMPACTION_DEBT_SCHEDULER.md hat/hatCache/compaction_debt_scheduler.go hat/hatCache/tr013_compaction_debt_scheduler_test.go hat/hatCache/tr013_compaction_debt_scheduler_benchmark_test.go scripts/run-tr013-compaction-debt.sh Makefile
        ;;
    stage)
        git add BENCHMARK.md INSPIRATION_BACKLOG.md README.md TR013_COMPACTION_DEBT_SCHEDULER.md hat/hatCache/compaction_debt_scheduler.go hat/hatCache/tr013_compaction_debt_scheduler_benchmark_test.go hat/hatCache/tr013_compaction_debt_scheduler_test.go scripts/run-tr013-compaction-debt.sh
        git apply --cached --check /tmp/tr013-compaction-debt-makefile.patch
        git apply --cached /tmp/tr013-compaction-debt-makefile.patch
        ;;
    commit)
        git commit -m 'feat: add compaction debt scheduler'
        ;;
    push)
        git push origin HEAD:master
        ;;
    status)
        git status --short --branch
        ;;
    *)
        printf '%s\n' 'usage: run-tr013-compaction-debt.sh {format|test|race|benchmark|vet|package|review|stage|commit|push|status}' >&2
        exit 2
        ;;
esac
