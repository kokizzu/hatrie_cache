#!/usr/bin/env bash
set -euo pipefail

paths=(
  BENCHMARK.md
  Makefile
  PRODUCT_IDEA_GAPS.md
  README.md
  TU39_SPACE_CHANGEFEED.md
  hat/hatReplication/space_changefeed.go
  hat/hatReplication/space_changefeed_example_test.go
  hat/hatReplication/space_changefeed_test.go
  scripts/benchmark-tu39.sh
  scripts/deliver-tu39.sh
  scripts/format-tu39.sh
  scripts/memory-tu39.sh
  scripts/run-tu39-focused.sh
  scripts/status-tu39.sh
  scripts/verify-tu39-docs.sh
  scripts/verify-tu39-focused.sh
)

case "${1:-}" in
  stage)
    git add "${paths[@]}"
    git diff --cached --check
    git status --short
    git diff --cached --stat
    ;;
  commit)
    git diff --cached --check
    git commit -m "feat(replication): add bounded space changefeeds [skip ci]"
    ;;
  push)
    git push -u origin codex/t-u39-space-changefeed-v2
    ;;
  *)
    printf 'usage: %s {stage|commit|push}\n' "$0" >&2
    exit 2
    ;;
esac
