#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master
cp "$repo/scripts/verify-m085.sh" "$tmp/scripts/verify-m085.sh"
cp "$repo/scripts/deliver-m085-checklist.sh" "$tmp/scripts/deliver-m085-checklist.sh"

if ! rg -q '^verify-m085:' "$tmp/Makefile"; then
	printf '\nverify-m085:\n\tsh ./scripts/verify-m085.sh\n\ndeliver-m085-checklist:\n\tsh ./scripts/deliver-m085-checklist.sh\n' >> "$tmp/Makefile"
fi

awk '
$0 == "- [ ] M085 Per-collection size and compaction metrics." {
	print "- [x] M085 Per-collection size and compaction metrics (see COLLECTION_METRICS.md)."
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/INSPIRATION.md" > "$tmp/INSPIRATION.md.new"
mv "$tmp/INSPIRATION.md.new" "$tmp/INSPIRATION.md"

awk '
index($0, "OPERATOR_MEMORY.md") != 0 {
	print
	print "- Per-collection size and compaction metrics: [collection metrics](COLLECTION_METRICS.md)"
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/README.md" > "$tmp/README.md.new"
mv "$tmp/README.md.new" "$tmp/README.md"

git -C "$tmp" add INSPIRATION.md README.md Makefile scripts/deliver-m085-checklist.sh scripts/verify-m085.sh
git -C "$tmp" diff --cached --check
git -C "$tmp" commit -m "docs(inspiration): mark collection metrics verified"
git -C "$tmp" push origin HEAD:master
