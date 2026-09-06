#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)

cleanup() {
	git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

git -C "$repo" fetch origin master >/dev/null
git -C "$repo" worktree add --detach "$tmp" origin/master >/dev/null

checked=$(awk '/^- \[x\] / {n++} END {print n + 0}' "$tmp/INSPIRATION.md")
unchecked=$(awk '/^- \[ \] / {n++} END {print n + 0}' "$tmp/INSPIRATION.md")
total=$((checked + unchecked))
commit=$(git -C "$tmp" rev-parse --short HEAD)

printf 'origin/master=%s\n' "$commit"
printf 'checked=%s\n' "$checked"
printf 'unchecked=%s\n' "$unchecked"
printf 'total=%s\n' "$total"
