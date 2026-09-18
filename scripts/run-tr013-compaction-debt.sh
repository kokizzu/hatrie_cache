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
        makefile_patch="$(mktemp)"
        trap 'rm -f "$makefile_patch"' EXIT
        tab=$'\t'
        printf '%s\n' \
            'diff --git a/Makefile b/Makefile' \
            '--- a/Makefile' \
            '+++ b/Makefile' \
            '@@ -18459,4 +18459,48 @@' \
            ' status-mz41-recursive-differential:' \
            "+${tab}bash ./scripts/run-mz41-recursive-differential.sh status" \
            ' ' \
            '+.PHONY: test-tr013-compaction-debt' \
            '+test-tr013-compaction-debt:' \
            "+${tab}bash ./scripts/run-tr013-compaction-debt.sh test" \
            '+' \
            '+.PHONY: format-tr013-compaction-debt' \
            '+format-tr013-compaction-debt:' \
            "+${tab}bash ./scripts/run-tr013-compaction-debt.sh format" \
            '+' \
            '+.PHONY: race-tr013-compaction-debt' \
            '+race-tr013-compaction-debt:' \
            "+${tab}bash ./scripts/run-tr013-compaction-debt.sh race" \
            '+' \
            '+.PHONY: benchmark-tr013-compaction-debt' \
            '+benchmark-tr013-compaction-debt:' \
            "+${tab}bash ./scripts/run-tr013-compaction-debt.sh benchmark" \
            '+' \
            '+.PHONY: vet-tr013-compaction-debt' \
            '+vet-tr013-compaction-debt:' \
            "+${tab}bash ./scripts/run-tr013-compaction-debt.sh vet" \
            '+' \
            '+.PHONY: package-tr013-compaction-debt' \
            '+package-tr013-compaction-debt:' \
            "+${tab}bash ./scripts/run-tr013-compaction-debt.sh package" \
            '+' \
            '+.PHONY: review-tr013-compaction-debt' \
            '+review-tr013-compaction-debt:' \
            "+${tab}bash ./scripts/run-tr013-compaction-debt.sh review" \
            '+' \
            '+.PHONY: stage-tr013-compaction-debt' \
            '+stage-tr013-compaction-debt:' \
            "+${tab}bash ./scripts/run-tr013-compaction-debt.sh stage" \
            '+' \
            '+.PHONY: commit-tr013-compaction-debt' \
            '+commit-tr013-compaction-debt:' \
            "+${tab}bash ./scripts/run-tr013-compaction-debt.sh commit" \
            '+' \
            '+.PHONY: push-tr013-compaction-debt' \
            '+push-tr013-compaction-debt:' \
            "+${tab}bash ./scripts/run-tr013-compaction-debt.sh push" \
            '+' \
            '+.PHONY: status-tr013-compaction-debt' \
            '+status-tr013-compaction-debt:' \
            "+${tab}bash ./scripts/run-tr013-compaction-debt.sh status" \
            '+' \
            ' .PHONY: test-chu27-priority' \
            > "$makefile_patch"
        git apply --cached --check "$makefile_patch"
        git apply --cached "$makefile_patch"
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
