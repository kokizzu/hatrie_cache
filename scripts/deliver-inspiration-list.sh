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
mkdir -p "$tmp/scripts"
cp "$repo/scripts/list-inspiration-open.sh" "$tmp/scripts/"
cp "$repo/scripts/deliver-inspiration-list.sh" "$tmp/scripts/"

if ! grep -q '^list-inspiration-open:' "$tmp/Makefile"; then
	printf '\nlist-inspiration-open:\n\tsh ./scripts/list-inspiration-open.sh\n\ndeliver-inspiration-list:\n\tsh ./scripts/deliver-inspiration-list.sh\n' >> "$tmp/Makefile"
fi

awk '/^- \[ \] / {print}' "$tmp/INSPIRATION.md" >/dev/null
git -C "$tmp" add Makefile scripts/list-inspiration-open.sh scripts/deliver-inspiration-list.sh
git -C "$tmp" commit -m "chore(tooling): list open inspiration items"
git -C "$tmp" push origin HEAD:master
