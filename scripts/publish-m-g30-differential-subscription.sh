#!/usr/bin/env bash
set -euo pipefail

feature_commit="$(git rev-parse "${1:-HEAD}^{commit}")"
paths=(
  M30_DIFFERENTIAL_SUBSCRIPTIONS.md
  hat/hatSql/differential_subscription.go
  hat/hatSql/differential_subscription_benchmark_test.go
  hat/hatSql/differential_subscription_public_test.go
  hat/hatSql/differential_subscription_test.go
  hat/hatSql/subscription.go
  scripts/benchmark-m-g30-differential-subscription.sh
  scripts/commit-m-g30-differential-subscription.sh
  scripts/format-m-g30-differential-subscription.sh
  scripts/inspect-m-g30-context.sh
  scripts/publish-m-g30-differential-subscription.sh
  scripts/race-m-g30-differential-subscription.sh
  scripts/test-m-g30-differential-subscription.sh
  scripts/test-m-g30-package.sh
  scripts/vet-m-g30-differential-subscription.sh
)

git cat-file -e "${feature_commit}^{commit}"
remote_dir="$(mktemp -d /tmp/hatrie_cache_m_g30_remote.XXXXXX)"

cleanup() {
  git worktree remove --force "$remote_dir" >/dev/null 2>&1 || true
  rm -rf "$remote_dir"
}
trap cleanup EXIT

git fetch origin master
git worktree add --detach "$remote_dir" origin/master
git -C "$remote_dir" checkout "$feature_commit" -- "${paths[@]}"
git -C "$remote_dir" diff --check -- "${paths[@]}"
go -C "$remote_dir" test -count=1 ./hat/hatSql -run '^Test(QueryDifferentialSubscription|QuerySubscription)'
git -C "$remote_dir" add -- "${paths[@]}"
git -C "$remote_dir" commit -m 'feat(sql): add differential subscriptions'
git -C "$remote_dir" push origin HEAD:master
git -C "$remote_dir" rev-parse HEAD
