#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master
cp "$repo/scripts/verify-c060.sh" "$tmp/scripts/verify-c060.sh"
cp "$repo/scripts/deliver-c060-checklist.sh" "$tmp/scripts/deliver-c060-checklist.sh"

if ! rg -q '^verify-c060:' "$tmp/Makefile"; then
	printf '\nverify-c060:\n\tsh ./scripts/verify-c060.sh\n\ndeliver-c060-checklist:\n\tsh ./scripts/deliver-c060-checklist.sh\n' >> "$tmp/Makefile"
fi

awk '
$0 == "- [ ] C060 Compression ratio and decompression CPU accounting." {
	print "- [x] C060 Compression ratio and decompression CPU accounting (see CODEC_METRICS.md)."
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/INSPIRATION.md" > "$tmp/INSPIRATION.md.new"
mv "$tmp/INSPIRATION.md.new" "$tmp/INSPIRATION.md"

awk '
index($0, "CODEC_SELECTION.md") != 0 {
	print
	print "- Codec byte and CPU accounting: [codec metrics](CODEC_METRICS.md)"
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/README.md" > "$tmp/README.md.new"
mv "$tmp/README.md.new" "$tmp/README.md"

git -C "$tmp" add INSPIRATION.md README.md Makefile scripts/deliver-c060-checklist.sh scripts/verify-c060.sh
git -C "$tmp" diff --cached --check
git -C "$tmp" commit -m "docs(inspiration): mark codec accounting verified"
git -C "$tmp" push origin HEAD:master
