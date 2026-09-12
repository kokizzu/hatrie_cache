#!/usr/bin/env bash
set -euo pipefail

feature_commit="$(git rev-parse "${1:-HEAD}^{commit}")"
remote_dir="$(mktemp -d /tmp/hatrie_cache_t_g06_remote.XXXXXX)"
paths=(
  T06_HASH_INDEX.md
  hat/hatDataStructure/hash_index.go
  hat/hatDataStructure/hash_index_benchmark_test.go
  hat/hatDataStructure/hash_index_public_test.go
  hat/hatDataStructure/hash_index_test.go
  scripts/benchmark-t-g06-hash-index.sh
  scripts/commit-t-g06-hash-index.sh
  scripts/format-t-g06-hash-index.sh
  scripts/inspect-t-g06-context.sh
  scripts/publish-t-g06-hash-index.sh
  scripts/race-t-g06-hash-index.sh
  scripts/test-t-g06-hash-index.sh
  scripts/test-t-g06-package.sh
  scripts/vet-t-g06-hash-index.sh
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
go -C "$remote_dir" test -count=1 ./hat/hatDataStructure -run '^TestHashIndex'
git -C "$remote_dir" add -- "${paths[@]}"
git -C "$remote_dir" commit -m 'feat(index): add typed hash index'
git -C "$remote_dir" push origin HEAD:master
git -C "$remote_dir" rev-parse HEAD
