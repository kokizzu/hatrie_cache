#!/usr/bin/env bash
set -euo pipefail

worktree="/tmp/hatrie-cache-tt021-parent"
if [[ -e "$worktree" ]]; then
  printf 'refusing to reuse existing path: %s\n' "$worktree" >&2
  exit 2
fi

parent="$(git rev-parse HEAD^ )"
cleanup() {
  git worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git worktree add --detach "$worktree" "$parent" >/dev/null
(cd "$worktree" && go test ./hat/hatDataStructure)
