#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master
cp "$repo/scripts/verify-m087.sh" "$tmp/scripts/verify-m087.sh"
cp "$repo/scripts/deliver-m087-checklist.sh" "$tmp/scripts/deliver-m087-checklist.sh"

if ! rg -q '^verify-m087:' "$tmp/Makefile"; then
	printf '\nverify-m087:\n\tsh ./scripts/verify-m087.sh\n\ndeliver-m087-checklist:\n\tsh ./scripts/deliver-m087-checklist.sh\n' >> "$tmp/Makefile"
fi

awk '
$0 == "- [ ] M087 Deterministic replica replay checks." {
	print "- [x] M087 Deterministic replica replay checks (see REPLAY_DIGEST.md)."
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/INSPIRATION.md" > "$tmp/INSPIRATION.md.new"
mv "$tmp/INSPIRATION.md.new" "$tmp/INSPIRATION.md"

awk '
index($0, "COLLECTION_METRICS.md") != 0 {
	print
	print "- Deterministic replica replay checks: [replay digest](REPLAY_DIGEST.md)"
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/README.md" > "$tmp/README.md.new"
mv "$tmp/README.md.new" "$tmp/README.md"

git -C "$tmp" add INSPIRATION.md README.md Makefile scripts/deliver-m087-checklist.sh scripts/verify-m087.sh
git -C "$tmp" diff --cached --check
git -C "$tmp" commit -m "docs(inspiration): mark deterministic replay checks verified"
git -C "$tmp" push origin HEAD:master
