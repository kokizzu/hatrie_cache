#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master
cp "$repo/FAILURE_DOMAIN_PLACEMENT.md" "$tmp/FAILURE_DOMAIN_PLACEMENT.md"
cp "$repo/scripts/verify-c159.sh" "$tmp/scripts/verify-c159.sh"
cp "$repo/scripts/deliver-c159-checklist.sh" "$tmp/scripts/deliver-c159-checklist.sh"

if ! rg -q '^verify-c159:' "$tmp/Makefile"; then
	printf '\nverify-c159:\n\tsh ./scripts/verify-c159.sh\n\ndeliver-c159-checklist:\n\tsh ./scripts/deliver-c159-checklist.sh\n' >> "$tmp/Makefile"
fi

awk '
$0 == "- [ ] C159 Failure-domain-aware replica placement." {
	print "- [x] C159 Failure-domain-aware replica placement (see FAILURE_DOMAIN_PLACEMENT.md)."
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/INSPIRATION.md" > "$tmp/INSPIRATION.md.new"
mv "$tmp/INSPIRATION.md.new" "$tmp/INSPIRATION.md"

awk '
index($0, "CROSS_REGION_REPLICATION.md") != 0 {
	print
	print "- Failure-domain-aware replica placement: [failure-domain placement](FAILURE_DOMAIN_PLACEMENT.md)"
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/README.md" > "$tmp/README.md.new"
mv "$tmp/README.md.new" "$tmp/README.md"

git -C "$tmp" add FAILURE_DOMAIN_PLACEMENT.md INSPIRATION.md README.md Makefile scripts/deliver-c159-checklist.sh scripts/verify-c159.sh
git -C "$tmp" diff --cached --check
git -C "$tmp" commit -m "docs(inspiration): mark failure-domain placement verified"
git -C "$tmp" push origin HEAD:master
