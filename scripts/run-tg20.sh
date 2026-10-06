#!/usr/bin/env bash
set -euo pipefail

mode=${1:-}
case "$mode" in
  baseline)
    go test ./hat/hatBackup -count=1
    ;;
  test)
    go test ./hat/hatBackup -count=1
    ;;
  race)
    go test -race ./hat/hatBackup -count=1
    ;;
  vet)
    go vet ./hat/hatBackup
    ;;
  format)
    gofmt -w hat/hatBackup/snapshot_rotation.go hat/hatBackup/tg20_snapshot_rotation_test.go
    ;;
  benchmark)
    go test ./hat/hatBackup -run '^$' -bench '^BenchmarkTG20' -benchmem -count=5
    ;;
  status)
    git status --short
    ;;
  stage)
    git add Makefile README.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md TG20_SNAPSHOT_ROTATION.md hat/hatBackup/snapshot_rotation.go hat/hatBackup/tg20_snapshot_rotation_test.go scripts/run-tg20.sh
    ;;
  commit)
    git commit -m 'feat: add opt-in snapshot rotation policy [skip ci]'
    ;;
  amend)
    git commit --amend --no-edit
    ;;
  push)
    git push --set-upstream origin "$(git branch --show-current)"
    ;;
  *)
    printf 'usage: %s baseline|test|race|vet|format|benchmark|status|stage|commit|amend|push\n' "$0" >&2
    exit 2
    ;;
esac
