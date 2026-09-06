#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" HEAD
cp "$repo/CODEC_SELECTION.md" "$tmp/CODEC_SELECTION.md"
cp "$repo/scripts/verify-c059.sh" "$tmp/scripts/verify-c059.sh"
cp "$repo/scripts/deliver-c059-checklist.sh" "$tmp/scripts/deliver-c059-checklist.sh"

if ! rg -q '^verify-c059:' "$tmp/Makefile"; then
	printf '\nverify-c059:\n\tsh ./scripts/verify-c059.sh\n\ndeliver-c059-checklist:\n\tsh ./scripts/deliver-c059-checklist.sh\n' >> "$tmp/Makefile"
fi

awk '
$0 == "- [ ] C059 Codec selection from sampled column entropy." {
	print "- [x] C059 Codec selection from sampled column entropy (see CODEC_SELECTION.md)."
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/INSPIRATION.md" > "$tmp/INSPIRATION.md.new"
mv "$tmp/INSPIRATION.md.new" "$tmp/INSPIRATION.md"

awk '
index($0, "GORILLA_FLOAT.md") != 0 {
	print
	print "- Entropy-based codec selection: [codec selection](CODEC_SELECTION.md)"
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/README.md" > "$tmp/README.md.new"
mv "$tmp/README.md.new" "$tmp/README.md"

git -C "$tmp" add CODEC_SELECTION.md INSPIRATION.md README.md Makefile scripts/deliver-c059-checklist.sh scripts/verify-c059.sh
git -C "$tmp" diff --cached --check
git -C "$tmp" commit -m "docs(inspiration): mark entropy codec selection verified"
git -C "$tmp" push origin HEAD:master
