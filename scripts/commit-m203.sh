#!/usr/bin/env bash
set -euo pipefail

git fetch origin master >/dev/null
if ! git diff --quiet HEAD origin/master; then
	printf '%s\n' 'origin/master moved while M203 was being prepared; refusing to publish automatically' >&2
	exit 1
fi

git add \
  Makefile \
  README.md \
  INSPIRATION_ROUND2.md \
  M203_SNAPSHOT_FREE_SUBSCRIPTIONS.md \
  hat/hatCache/journal_snapshot_free_benchmark_test.go \
  hat/hatCache/journal_snapshot_free_test.go \
  hat/hatCache/journal_subscription.go \
  scripts/benchmark-m203.sh \
  scripts/commit-m203.sh \
  scripts/format-m203.sh \
  scripts/race-m203.sh \
  scripts/test-m203-full.sh \
  scripts/test-m203-package.sh \
  scripts/test-m203-snapshot-free.sh \
  scripts/vet-m203.sh
git diff --cached --check
git commit -m 'feat(hatCache): add snapshot-free journal subscriptions'
git push origin HEAD:master
