#!/usr/bin/env bash
set -euo pipefail

mode=${1:-status}
expected_branch='codex/chg22-clickhouse-next'
commit_message='feat(replication): add bounded space changefeed [skip ci]'

files=(
  Makefile
  PRODUCT_IDEA_GAPS.md
  README.md
  TU39_SPACE_CHANGEFEED.md
  hat/hatReplication/space_changefeed.go
  hat/hatReplication/space_changefeed_test.go
  hat/hatReplication/space_changefeed_baseline_benchmark_test.go
  hat/hatReplication/space_changefeed_benchmark_test.go
  scripts/test-chg22-changefeed.sh
  scripts/deliver-chg22-changefeed.sh
)

require_branch() {
  branch=$(git branch --show-current)
  if [[ "$branch" != "$expected_branch" ]]; then
    printf 'wrong branch: got %s, want %s\n' "$branch" "$expected_branch" >&2
    exit 1
  fi
}

run_verify() {
  make format-chg22-changefeed
  make test-chg22-changefeed
  make race-chg22-changefeed
  make vet-chg22-changefeed
  make package-chg22-changefeed
  make benchmark-chg22-changefeed
}

case "$mode" in
  status)
    require_branch
    git status --short
    ;;
  verify)
    require_branch
    run_verify
    ;;
  stage)
    require_branch
    git add "${files[@]}"
    git diff --cached --check
    git status --short
    ;;
  commit)
    require_branch
    git add "${files[@]}"
    git diff --cached --check
    git commit -m "$commit_message"
    ;;
  push)
    require_branch
    git push origin "$expected_branch"
    ;;
  *)
    printf 'usage: %s {status|verify|stage|commit|push}\n' "$0" >&2
    exit 2
    ;;
esac
