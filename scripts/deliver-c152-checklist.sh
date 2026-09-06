#!/bin/sh
set -eu

repo=${1:-/home/kyz/go/src/hatrie_cache}
tmp=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true' EXIT

git -C "$repo" worktree add --detach --quiet "$tmp" origin/master
cp "$repo/LEADER_ELECTION.md" "$tmp/LEADER_ELECTION.md"
cp "$repo/scripts/verify-c152.sh" "$tmp/scripts/verify-c152.sh"
cp "$repo/scripts/deliver-c152-checklist.sh" "$tmp/scripts/deliver-c152-checklist.sh"

if ! rg -q '^verify-c152:' "$tmp/Makefile"; then
	printf '\nverify-c152:\n\tsh ./scripts/verify-c152.sh\n\ndeliver-c152-checklist:\n\tsh ./scripts/deliver-c152-checklist.sh\n' >> "$tmp/Makefile"
fi

awk '
$0 == "- [ ] C152 Leader election independent from query workers." {
	print "- [x] C152 Leader election independent from query workers (see LEADER_ELECTION.md)."
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/INSPIRATION.md" > "$tmp/INSPIRATION.md.new"
mv "$tmp/INSPIRATION.md.new" "$tmp/INSPIRATION.md"

awk '
index($0, "OBJECT_STORE_BACKUP.md") != 0 {
	print
	print "- Leader election independent from query workers: [leader election](LEADER_ELECTION.md)"
	found++
	next
}
{ print }
END { if (found != 1) exit 1 }
' "$tmp/README.md" > "$tmp/README.md.new"
mv "$tmp/README.md.new" "$tmp/README.md"

git -C "$tmp" add LEADER_ELECTION.md INSPIRATION.md README.md Makefile scripts/deliver-c152-checklist.sh scripts/verify-c152.sh
git -C "$tmp" diff --cached --check
git -C "$tmp" commit -m "docs(inspiration): mark independent leader election verified"
git -C "$tmp" push origin HEAD:master
