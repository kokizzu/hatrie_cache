#!/usr/bin/env bash
set -eu

case "${1:-}" in
    format)
        gofmt -w hat/hatTopology/tr01_leader_lease.go hat/hatTopology/tr01_leader_lease_test.go hat/hatTopology/tr01_leader_lease_benchmark_test.go
        ;;
    test)
        go test ./hat/hatTopology -run 'TestTR01LeaderLease'
        ;;
    race)
        go test -race ./hat/hatTopology -run 'TestTR01LeaderLease'
        ;;
    benchmark)
        go test ./hat/hatTopology -run '^$' -bench 'BenchmarkTR01' -benchmem -count=5
        ;;
    vet)
        go vet ./hat/hatTopology
        ;;
    package)
        go test ./hat/hatTopology
        ;;
    review)
        git diff --check
        git diff --cached --check
        git status --short
        git diff -- INSPIRATION_BACKLOG.md BENCHMARK.md README.md TR01_LEADER_LEASE.md hat/hatTopology/tr01_leader_lease.go hat/hatTopology/tr01_leader_lease_test.go hat/hatTopology/tr01_leader_lease_benchmark_test.go scripts/run-tr01-leader-lease.sh Makefile
        ;;
    stage)
        git add INSPIRATION_BACKLOG.md BENCHMARK.md README.md TR01_LEADER_LEASE.md hat/hatTopology/tr01_leader_lease.go hat/hatTopology/tr01_leader_lease_test.go hat/hatTopology/tr01_leader_lease_benchmark_test.go scripts/run-tr01-leader-lease.sh
        makefile_patch="$(mktemp)"
        trap 'rm -f "$makefile_patch"' EXIT
        tab=$'\t'
        printf '%s\n' \
            'diff --git a/Makefile b/Makefile' \
            '--- a/Makefile' \
            '+++ b/Makefile' \
            '@@ -18487,4 +18487,48 @@' \
            ' status-tr013-compaction-debt:' \
            " ${tab}bash ./scripts/run-tr013-compaction-debt.sh status" \
            ' ' \
            '+.PHONY: test-tr01-leader-lease' \
            '+test-tr01-leader-lease:' \
            "+${tab}bash ./scripts/run-tr01-leader-lease.sh test" \
            '+' \
            '+.PHONY: format-tr01-leader-lease' \
            '+format-tr01-leader-lease:' \
            "+${tab}bash ./scripts/run-tr01-leader-lease.sh format" \
            '+' \
            '+.PHONY: race-tr01-leader-lease' \
            '+race-tr01-leader-lease:' \
            "+${tab}bash ./scripts/run-tr01-leader-lease.sh race" \
            '+' \
            '+.PHONY: benchmark-tr01-leader-lease' \
            '+benchmark-tr01-leader-lease:' \
            "+${tab}bash ./scripts/run-tr01-leader-lease.sh benchmark" \
            '+' \
            '+.PHONY: vet-tr01-leader-lease' \
            '+vet-tr01-leader-lease:' \
            "+${tab}bash ./scripts/run-tr01-leader-lease.sh vet" \
            '+' \
            '+.PHONY: package-tr01-leader-lease' \
            '+package-tr01-leader-lease:' \
            "+${tab}bash ./scripts/run-tr01-leader-lease.sh package" \
            '+' \
            '+.PHONY: review-tr01-leader-lease' \
            '+review-tr01-leader-lease:' \
            "+${tab}bash ./scripts/run-tr01-leader-lease.sh review" \
            '+' \
            '+.PHONY: stage-tr01-leader-lease' \
            '+stage-tr01-leader-lease:' \
            "+${tab}bash ./scripts/run-tr01-leader-lease.sh stage" \
            '+' \
            '+.PHONY: commit-tr01-leader-lease' \
            '+commit-tr01-leader-lease:' \
            "+${tab}bash ./scripts/run-tr01-leader-lease.sh commit" \
            '+' \
            '+.PHONY: push-tr01-leader-lease' \
            '+push-tr01-leader-lease:' \
            "+${tab}bash ./scripts/run-tr01-leader-lease.sh push" \
            '+' \
            '+.PHONY: status-tr01-leader-lease' \
            '+status-tr01-leader-lease:' \
            "+${tab}bash ./scripts/run-tr01-leader-lease.sh status" \
            '+' \
            ' .PHONY: test-chu27-priority' \
            > "$makefile_patch"
        git apply --cached --check "$makefile_patch"
        git apply --cached "$makefile_patch"
        ;;
    commit)
        git commit -m 'feat: add leader lease fencing primitive'
        ;;
    push)
        git push origin HEAD:master
        ;;
    status)
        git status --short --branch
        ;;
    *)
        printf '%s\n' 'usage: run-tr01-leader-lease.sh {format|test|race|benchmark|vet|package|review|stage|commit|push|status}' >&2
        exit 2
        ;;
esac
