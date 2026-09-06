#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master
cp "$repo/scripts/verify-c128.sh" "$tmp/scripts/verify-c128.sh"
cp "$repo/scripts/deliver-c128-checklist.sh" "$tmp/scripts/deliver-c128-checklist.sh"

if ! rg -q '^verify-c128:' "$tmp/Makefile"; then
	printf '\nverify-c128:\n\tsh ./scripts/verify-c128.sh\n\ndeliver-c128-checklist:\n\tsh ./scripts/deliver-c128-checklist.sh\n' >> "$tmp/Makefile"
fi

awk '
$0 == "- [ ] C128 Object-store backup targets." {
	print "- [x] C128 Object-store backup targets (see OBJECT_STORE_BACKUP.md)."
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/INSPIRATION.md" > "$tmp/INSPIRATION.md.new"
mv "$tmp/INSPIRATION.md.new" "$tmp/INSPIRATION.md"

awk '
index($0, "COMPRESSION_NEGOTIATION.md") != 0 {
	print
	print "- Object-store backup targets: [object-store backup](OBJECT_STORE_BACKUP.md)"
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/README.md" > "$tmp/README.md.new"
mv "$tmp/README.md.new" "$tmp/README.md"

git -C "$tmp" add INSPIRATION.md README.md Makefile scripts/deliver-c128-checklist.sh scripts/verify-c128.sh
git -C "$tmp" diff --cached --check
git -C "$tmp" commit -m "docs(inspiration): mark object-store backups verified"
git -C "$tmp" push origin HEAD:master
