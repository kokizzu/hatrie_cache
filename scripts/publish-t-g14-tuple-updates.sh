#!/usr/bin/env bash
set -euo pipefail

feature_commit="$(git rev-parse "${1:-HEAD}^{commit}")"
remote_dir="$(mktemp -d /tmp/hatrie_cache_t_g14_remote.XXXXXX)"
paths=(
  T14_TUPLE_FIELD_UPDATES.md
  hat/hatDataStructure/tuple_field_updates.go
  hat/hatDataStructure/tuple_field_updates_benchmark_test.go
  hat/hatDataStructure/tuple_field_updates_public_test.go
  hat/hatDataStructure/tuple_field_updates_test.go
  scripts/benchmark-t-g14-tuple-updates.sh
  scripts/commit-t-g14-tuple-updates.sh
  scripts/format-t-g14-tuple-updates.sh
  scripts/inspect-t-g14-context.sh
  scripts/publish-t-g14-tuple-updates.sh
  scripts/race-t-g14-tuple-updates.sh
  scripts/test-t-g14-package.sh
  scripts/test-t-g14-tuple-updates.sh
  scripts/vet-t-g14-tuple-updates.sh
)

cleanup() {
  git worktree remove --force "$remote_dir" >/dev/null 2>&1 || true
  rm -rf "$remote_dir"
}
trap cleanup EXIT

git fetch origin master
git worktree add --detach "$remote_dir" origin/master
git -C "$remote_dir" checkout "$feature_commit" -- "${paths[@]}"
git -C "$remote_dir" diff --check -- "${paths[@]}"
go -C "$remote_dir" test -count=1 ./hat/hatDataStructure -run '^TestTupleFieldUpdates'
git -C "$remote_dir" add -- "${paths[@]}"
git -C "$remote_dir" commit -m 'feat(tuple): add atomic field updates'
git -C "$remote_dir" push origin HEAD:master
git -C "$remote_dir" rev-parse HEAD
