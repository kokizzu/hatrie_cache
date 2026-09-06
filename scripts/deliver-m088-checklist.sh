#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master
cp "$repo/scripts/verify-m088.sh" "$tmp/scripts/verify-m088.sh"
cp "$repo/scripts/deliver-m088-checklist.sh" "$tmp/scripts/deliver-m088-checklist.sh"

if ! rg -q '^verify-m088:' "$tmp/Makefile"; then
	printf '\nverify-m088:\n\tsh ./scripts/verify-m088.sh\n\ndeliver-m088-checklist:\n\tsh ./scripts/deliver-m088-checklist.sh\n' >> "$tmp/Makefile"
fi

awk '
$0 == "- [ ] M088 Read replicas with explicit staleness bounds." {
	print "- [x] M088 Read replicas with explicit staleness bounds (see READ_REPLICA_POLICY.md)."
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/INSPIRATION.md" > "$tmp/INSPIRATION.md.new"
mv "$tmp/INSPIRATION.md.new" "$tmp/INSPIRATION.md"

awk '
index($0, "REPLAY_DIGEST.md") != 0 {
	print
	print "- Read replicas with explicit staleness bounds: [read-replica policy](READ_REPLICA_POLICY.md)"
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/README.md" > "$tmp/README.md.new"
mv "$tmp/README.md.new" "$tmp/README.md"

git -C "$tmp" add INSPIRATION.md README.md Makefile scripts/deliver-m088-checklist.sh scripts/verify-m088.sh
git -C "$tmp" diff --cached --check
git -C "$tmp" commit -m "docs(inspiration): mark read replica policy verified"
git -C "$tmp" push origin HEAD:master
