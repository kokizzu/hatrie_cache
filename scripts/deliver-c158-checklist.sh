#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master
cp "$repo/SPLIT_BRAIN_FENCING.md" "$tmp/SPLIT_BRAIN_FENCING.md"
cp "$repo/scripts/verify-c158.sh" "$tmp/scripts/verify-c158.sh"
cp "$repo/scripts/deliver-c158-checklist.sh" "$tmp/scripts/deliver-c158-checklist.sh"

if ! rg -q '^verify-c158:' "$tmp/Makefile"; then
	printf '\nverify-c158:\n\tsh ./scripts/verify-c158.sh\n\ndeliver-c158-checklist:\n\tsh ./scripts/deliver-c158-checklist.sh\n' >> "$tmp/Makefile"
fi

awk '
$0 == "- [ ] C158 Split-brain fencing." {
	print "- [x] C158 Split-brain fencing (see SPLIT_BRAIN_FENCING.md)."
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/INSPIRATION.md" > "$tmp/INSPIRATION.md.new"
mv "$tmp/INSPIRATION.md.new" "$tmp/INSPIRATION.md"

awk '
index($0, "LEADER_ELECTION.md") != 0 {
	print
	print "- Split-brain fencing tokens: [split-brain fencing](SPLIT_BRAIN_FENCING.md)"
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/README.md" > "$tmp/README.md.new"
mv "$tmp/README.md.new" "$tmp/README.md"

git -C "$tmp" add SPLIT_BRAIN_FENCING.md INSPIRATION.md README.md Makefile scripts/deliver-c158-checklist.sh scripts/verify-c158.sh
git -C "$tmp" diff --cached --check
git -C "$tmp" commit -m "docs(inspiration): mark split-brain fencing verified"
git -C "$tmp" push origin HEAD:master
