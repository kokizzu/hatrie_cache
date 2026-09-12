#!/usr/bin/env bash
set -euo pipefail

feature_commit="$(git rev-parse "${1:-HEAD}^{commit}")"
remote_dir="$(mktemp -d /tmp/hatrie_cache_t_g05_remote.XXXXXX)"
paths=(
  T05_ORDERED_INDEX_ITERATOR.md
  hat/hatDataStructure/ordered_index.go
  hat/hatDataStructure/ordered_index_benchmark_test.go
  hat/hatDataStructure/ordered_index_public_test.go
  hat/hatDataStructure/ordered_index_test.go
  scripts/benchmark-t-g05-mutation.sh
  scripts/benchmark-t-g05-ordered-index.sh
  scripts/commit-t-g05-ordered-index.sh
  scripts/format-t-g05-ordered-index.sh
  scripts/inspect-t-g05-context.sh
  scripts/inspect-t-g05-source.sh
  scripts/race-t-g05-ordered-index.sh
  scripts/test-t-g05-ordered-index.sh
  scripts/test-t-g05-package.sh
  scripts/vet-t-g05-ordered-index.sh
  scripts/publish-t-g05-ordered-index.sh
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
go -C "$remote_dir" test -count=1 ./hat/hatDataStructure -run '^TestOrderedIndex'
git -C "$remote_dir" add -- "${paths[@]}"
git -C "$remote_dir" commit -m 'feat(index): add ordered iterator'
git -C "$remote_dir" push origin HEAD:master
git -C "$remote_dir" rev-parse HEAD
