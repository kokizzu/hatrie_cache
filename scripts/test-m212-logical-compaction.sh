#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
worktree="${TMPDIR:-/tmp}/hatrie-m212-test.$PPID"

cleanup() {
  git -C "$repo_root" worktree remove --force "$worktree" >/dev/null 2>&1 || true
  rm -rf "$worktree"
}
trap cleanup EXIT

git -C "$repo_root" fetch origin master >/dev/null
git -C "$repo_root" worktree add --detach "$worktree" origin/master >/dev/null
cp "$repo_root/hat/hatDataStructure/differential_multiset.go" "$worktree/hat/hatDataStructure/differential_multiset.go"
cp "$repo_root/hat/hatDataStructure/logical_compaction.go" "$worktree/hat/hatDataStructure/logical_compaction.go"
cp "$repo_root/hat/hatDataStructure/m212_logical_compaction_test.go" "$worktree/hat/hatDataStructure/m212_logical_compaction_test.go"
(cd "$worktree" && go test \
  hat/hatDataStructure/differential_multiset.go \
  hat/hatDataStructure/logical_compaction.go \
  hat/hatDataStructure/m212_logical_compaction_test.go)
