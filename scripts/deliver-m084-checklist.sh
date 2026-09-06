#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master
cp "$repo/scripts/verify-m084.sh" "$tmp/scripts/verify-m084.sh"
cp "$repo/scripts/deliver-m084-checklist.sh" "$tmp/scripts/deliver-m084-checklist.sh"

if ! rg -q '^verify-m084:' "$tmp/Makefile"; then
	printf '\nverify-m084:\n\tsh ./scripts/verify-m084.sh\n\ndeliver-m084-checklist:\n\tsh ./scripts/deliver-m084-checklist.sh\n' >> "$tmp/Makefile"
fi

awk '
$0 == "- [ ] M084 Per-operator retained-memory metrics." {
	print "- [x] M084 Per-operator retained-memory metrics (see OPERATOR_MEMORY.md)."
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/INSPIRATION.md" > "$tmp/INSPIRATION.md.new"
mv "$tmp/INSPIRATION.md.new" "$tmp/INSPIRATION.md"

awk '
index($0, "FAILURE_DOMAIN_PLACEMENT.md") != 0 {
	print
	print "- Per-operator retained-memory metrics: [operator memory](OPERATOR_MEMORY.md)"
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/README.md" > "$tmp/README.md.new"
mv "$tmp/README.md.new" "$tmp/README.md"

git -C "$tmp" add INSPIRATION.md README.md Makefile scripts/deliver-m084-checklist.sh scripts/verify-m084.sh
git -C "$tmp" diff --cached --check
git -C "$tmp" commit -m "docs(inspiration): mark operator memory metrics verified"
git -C "$tmp" push origin HEAD:master
