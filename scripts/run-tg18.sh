#!/usr/bin/env bash
set -euo pipefail

mode=${1:-}
case "$mode" in
  format)
    gofmt -w hat/hatJournal/journal.go hat/hatJournal/sync_policy.go \
      hat/hatJournal/tr018_sync_policy_test.go hat/hatJournal/tr018_sync_policy_benchmark_test.go \
      hat/hatCache/journal.go hat/hatCache/tr018_command_journal_sync_mode_test.go
    ;;
  test)
    go test ./hat/hatJournal -count=1
    ;;
  test-cache)
    go test ./hat/hatCache -count=1
    ;;
  race)
    go test -race ./hat/hatJournal -count=1
    ;;
  vet)
    go vet ./hat/hatJournal
    ;;
  benchmark)
    go test ./hat/hatJournal -run '^$' -bench '^BenchmarkTR018SyncPolicyDecision$' -benchmem -count=5
    ;;
  status)
    git status --short
    ;;
  stage)
    git add Makefile README.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md \
      TR018_WAL_SYNC_MODES.md \
      hat/hatJournal/journal.go hat/hatJournal/sync_policy.go \
      hat/hatJournal/tr018_sync_policy_test.go \
      hat/hatJournal/tr018_sync_policy_benchmark_test.go \
      hat/hatCache/journal.go hat/hatCache/tr018_command_journal_sync_mode_test.go \
      scripts/run-tg18.sh
    git diff --cached --check
    ;;
  commit)
    git diff --cached --check
    git commit -m 'feat: add configurable WAL sync modes [skip ci]'
    ;;
  push)
    git push -u origin HEAD
    ;;
  *)
    printf 'usage: %s {format|test|test-cache|race|vet|benchmark|status|stage|commit|push}\n' "$0" >&2
    exit 2
    ;;
esac
