#!/usr/bin/env bash
set -euo pipefail

branch="codex/chg19-clickhouse-next"
message="feat(journal): add per-space WAL sync policy [skip ci]"
paths=(
  Makefile
  PRODUCT_IDEA_GAPS.md
  README.md
  TU34_PER_SPACE_WAL_SYNC.md
  hat/hatCache/journal.go
  hat/hatCache/journal_durability.go
  hat/hatCache/journal_sync_policy.go
  hat/hatCache/monitoring.go
  hat/hatCache/t_u34_per_space_wal_benchmark_test.go
  hat/hatCache/t_u34_per_space_wal_test.go
  hat/hatJournal/journal.go
  hat/hatJournal/sync_policy.go
  hat/hatJournal/sync_policy_test.go
  scripts/deliver-chg19-wal.sh
  scripts/test-chg19-wal.sh
)

case "${1:-status}" in
  status)
    git status --short
    ;;
  verify)
    git diff --check
    git status --short
    ;;
  stage)
    git add "${paths[@]}"
    git diff --cached --check
    git diff --cached --stat
    ;;
  commit)
    git commit -m "$message"
    ;;
  push)
    git push -u origin "$branch"
    ;;
  *)
    printf 'usage: %s [status|verify|stage|commit|push]\n' "$0" >&2
    exit 2
    ;;
esac
