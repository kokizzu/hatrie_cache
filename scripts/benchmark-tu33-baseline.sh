#!/usr/bin/env bash
set -euo pipefail

baseline=/tmp/hatrie-cache-tu33-baseline
if [[ -e "$baseline" ]]; then
  printf 'baseline worktree already exists: %s\n' "$baseline" >&2
  exit 1
fi
cleanup() {
  git worktree remove --force "$baseline" >/dev/null 2>&1 || true
}
trap cleanup EXIT
git worktree add --detach "$baseline" origin/master >/dev/null
go -C "$baseline" test ./hat/hatAuth -run '^$' -bench '^BenchmarkMU021AfterRoleCatalogAuthorize$' -benchmem -count=5
