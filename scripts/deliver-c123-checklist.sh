#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master
cp "$repo/scripts/verify-c123.sh" "$tmp/scripts/verify-c123.sh"
cp "$repo/scripts/deliver-c123-checklist.sh" "$tmp/scripts/deliver-c123-checklist.sh"

if ! rg -q '^verify-c123:' "$tmp/Makefile"; then
	printf '\nverify-c123:\n\tsh ./scripts/verify-c123.sh\n\ndeliver-c123-checklist:\n\tsh ./scripts/deliver-c123-checklist.sh\n' >> "$tmp/Makefile"
fi

awk '
$0 == "- [ ] C123 Compression level negotiation per client." {
	print "- [x] C123 Compression level negotiation per client (see COMPRESSION_NEGOTIATION.md)."
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
	print "- Per-client compression-level negotiation: [compression negotiation](COMPRESSION_NEGOTIATION.md)"
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/README.md" > "$tmp/README.md.new"
mv "$tmp/README.md.new" "$tmp/README.md"

git -C "$tmp" add INSPIRATION.md README.md Makefile scripts/deliver-c123-checklist.sh scripts/verify-c123.sh
git -C "$tmp" diff --cached --check
git -C "$tmp" commit -m "docs(inspiration): mark compression negotiation verified"
git -C "$tmp" push origin HEAD:master
