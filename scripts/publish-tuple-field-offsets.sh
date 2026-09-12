#!/usr/bin/env bash
set -euo pipefail

feature_commit="$(git rev-parse "${1:-HEAD}^{commit}")"
paths=(
  TR19_TUPLE_FIELD_OFFSETS.md
  hat/hatDataStructure/tuple_field_offsets.go
  hat/hatDataStructure/tuple_field_offsets_public_test.go
  hat/hatDataStructure/tuple_field_offsets_test.go
  scripts/benchmark-tuple-field-offsets.sh
  scripts/commit-tuple-field-offsets.sh
  scripts/format-tuple-field-offsets.sh
  scripts/publish-tuple-field-offsets.sh
  scripts/race-tuple-field-offsets.sh
  scripts/test-tuple-field-offsets-package.sh
  scripts/test-tuple-field-offsets.sh
  scripts/vet-tuple-field-offsets.sh
)

git cat-file -e "${feature_commit}^{commit}"
remote_dir="$(mktemp -d /tmp/hatrie_cache_tuple_offsets_remote.XXXXXX)"

cleanup() {
  git worktree remove --force "$remote_dir" >/dev/null 2>&1 || true
  rm -rf "$remote_dir"
}
trap cleanup EXIT

git fetch origin master
git worktree add --detach "$remote_dir" origin/master
git -C "$remote_dir" checkout "$feature_commit" -- "${paths[@]}"
git -C "$remote_dir" diff --check -- "${paths[@]}"
go -C "$remote_dir" test -count=1 ./hat/hatDataStructure -run 'Test(TupleFieldOffsetCache|PackedTuple)'
git -C "$remote_dir" add -- "${paths[@]}"
git -C "$remote_dir" commit -m 'feat(tuple): add cached field offsets'
git -C "$remote_dir" push origin HEAD:master
git -C "$remote_dir" rev-parse HEAD
