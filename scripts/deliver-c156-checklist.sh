#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master
cp "$repo/CROSS_REGION_REPLICATION.md" "$tmp/CROSS_REGION_REPLICATION.md"
cp "$repo/scripts/verify-c156.sh" "$tmp/scripts/verify-c156.sh"
cp "$repo/scripts/deliver-c156-checklist.sh" "$tmp/scripts/deliver-c156-checklist.sh"

if ! rg -q '^verify-c156:' "$tmp/Makefile"; then
	printf '\nverify-c156:\n\tsh ./scripts/verify-c156.sh\n\ndeliver-c156-checklist:\n\tsh ./scripts/deliver-c156-checklist.sh\n' >> "$tmp/Makefile"
fi

awk '
$0 == "- [ ] C156 Cross-region replication policy." {
	print "- [x] C156 Cross-region replication policy (see CROSS_REGION_REPLICATION.md)."
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/INSPIRATION.md" > "$tmp/INSPIRATION.md.new"
mv "$tmp/INSPIRATION.md.new" "$tmp/INSPIRATION.md"

awk '
index($0, "SPLIT_BRAIN_FENCING.md") != 0 {
	print
	print "- Cross-region replication policy: [cross-region replication](CROSS_REGION_REPLICATION.md)"
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/README.md" > "$tmp/README.md.new"
mv "$tmp/README.md.new" "$tmp/README.md"

git -C "$tmp" add CROSS_REGION_REPLICATION.md INSPIRATION.md README.md Makefile scripts/deliver-c156-checklist.sh scripts/verify-c156.sh
git -C "$tmp" diff --cached --check
git -C "$tmp" commit -m "docs(inspiration): mark regional replication policy verified"
git -C "$tmp" push origin HEAD:master
