#!/usr/bin/env bash
set -euo pipefail

mode="${1:-preview}"
case "$mode" in
preview|commit|push) ;;
*)
	echo "usage: $0 [preview|commit|push]" >&2
	exit 2
	;;
esac

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

tmpdir="$root/.delivery-disk-placement.$$"
index="$root/.git/disk-placement-index.$$"
cleanup() {
	rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT

mkdir -p -- "$tmpdir"
git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"

cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-disk-placement test-disk-placement test-race-disk-placement benchmark-disk-placement deliver-disk-placement commit-disk-placement push-disk-placement
format-disk-placement:
	bash ./scripts/format-disk-placement.sh
test-disk-placement:
	bash ./scripts/test-disk-placement.sh
test-race-disk-placement:
	bash ./scripts/test-race-disk-placement.sh
benchmark-disk-placement:
	bash ./scripts/benchmark-disk-placement.sh
deliver-disk-placement:
	bash ./scripts/deliver-disk-placement.sh preview
commit-disk-placement:
	bash ./scripts/deliver-disk-placement.sh commit
push-disk-placement:
	bash ./scripts/deliver-disk-placement.sh push
EOF

awk '
{
	print
	if (!added && $0 ~ /^- \[ \] C041 /) {
		print "- [x] C041a Immutable weighted deterministic disk placement policy with duplicate/overflow validation."
		added = 1
	}
}
END {
	if (!added) {
		print "missing C041 checklist row" > "/dev/stderr"
		exit 1
	}
}
' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
	DISK_PLACEMENT.md \
	hat/hatStorage/disk_placement.go \
	hat/hatStorage/disk_placement_test.go \
	scripts/benchmark-disk-placement.sh \
	scripts/deliver-disk-placement.sh \
	scripts/format-disk-placement.sh \
	scripts/test-disk-placement.sh \
	scripts/test-race-disk-placement.sh

for generated in Makefile INSPIRATION.md; do
	blob=$(git hash-object -w "$tmpdir/$generated")
	GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

echo "disk-placement delivery mode: $mode"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
	exit 0
fi

if [[ "$mode" == commit ]]; then
	GIT_INDEX_FILE="$index" git commit -m "feat(storage): add deterministic disk placement policy"
	exit 0
fi

git push origin HEAD:master
