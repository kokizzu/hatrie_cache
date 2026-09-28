#!/usr/bin/env bash
set -euo pipefail

root=$(git rev-parse --show-toplevel)
worktree=/tmp/hatrie-t250-durable-sequence-test.$$
trap 'git -C "$root" worktree remove --force "$worktree" >/dev/null 2>&1 || true; rm -rf "$worktree"' EXIT

git -C "$root" fetch origin
git -C "$root" worktree add --detach "$worktree" origin/master
cp "$root/hat/hatDataStructure/t250_durable_sequence_test.go" "$worktree/hat/hatDataStructure/t250_durable_sequence_test.go"
cp "$root/hat/hatDataStructure/durable_sequence.go" "$worktree/hat/hatDataStructure/durable_sequence.go"
go test "$worktree/hat/hatDataStructure/durable_sequence.go" "$worktree/hat/hatDataStructure/t250_durable_sequence_test.go"
