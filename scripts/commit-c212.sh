#!/usr/bin/env bash
set -euo pipefail

repo=/tmp/hatrie-cache-c212
base=a43b3f74beac834743a614421d42ea1df0ab5328

git -C "$repo" fetch origin master
if [[ "$(git -C "$repo" rev-parse HEAD)" != "$base" ]]; then
  printf 'refusing C212 commit from unexpected local base\n' >&2
  exit 1
fi
if [[ "$(git -C "$repo" rev-parse origin/master)" != "$base" ]]; then
  printf 'refusing C212 push because origin/master advanced\n' >&2
  exit 1
fi

git -C "$repo" diff --check
git -C "$repo" add -- \
  Makefile \
  README.md \
  INSPIRATION_ROUND2.md \
  C212_PRECOMPUTED_JOIN_HASH.md \
  hat/hatSql/c212_hash_join.go \
  hat/hatSql/c212_hash_join_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-c212-before.sh \
  scripts/benchmark-c212.sh \
  scripts/benchmark-c212-index.sh \
  scripts/commit-c212.sh \
  scripts/format-c212.sh \
  scripts/race-c212.sh \
  scripts/test-c212-full.sh \
  scripts/test-c212-package.sh \
  scripts/test-c212.sh \
  scripts/vet-c212.sh
git -C "$repo" diff --cached --check
git -C "$repo" commit -m 'feat(hatSql): optimize hash join probe keys'
git -C "$repo" push origin HEAD:master
