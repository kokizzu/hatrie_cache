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
awk '/^- \[ \] / {print}' "$tmp/INSPIRATION.md"
