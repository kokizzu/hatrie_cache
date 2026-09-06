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
cp "$repo/scripts/audit-inspiration-remote.sh" "$tmp/scripts/"
cp "$repo/scripts/deliver-inspiration-audit.sh" "$tmp/scripts/"

if ! grep -q '^audit-inspiration-remote:' "$tmp/Makefile"; then
	printf '\naudit-inspiration-remote:\n\tsh ./scripts/audit-inspiration-remote.sh\n\ndeliver-inspiration-audit:\n\tsh ./scripts/deliver-inspiration-audit.sh\n' >> "$tmp/Makefile"
fi

sh "$tmp/scripts/audit-inspiration-remote.sh"
git -C "$tmp" add Makefile scripts/audit-inspiration-remote.sh scripts/deliver-inspiration-audit.sh
git -C "$tmp" commit -m "chore(tooling): add inspiration checklist audit"
git -C "$tmp" push origin HEAD:master
