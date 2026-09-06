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

tmpdir="$root/.delivery-storage-tier.$$"
index="$root/.git/storage-tier-index.$$"
cleanup() {
	rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT

mkdir -p -- "$tmpdir"
git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"

cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-storage-tier test-storage-tier test-race-storage-tier benchmark-storage-tier deliver-storage-tier commit-storage-tier push-storage-tier
format-storage-tier:
	bash ./scripts/format-storage-tier.sh
test-storage-tier:
	bash ./scripts/test-storage-tier.sh
test-race-storage-tier:
	bash ./scripts/test-race-storage-tier.sh
benchmark-storage-tier:
	bash ./scripts/benchmark-storage-tier.sh
deliver-storage-tier:
	bash ./scripts/deliver-storage-tier.sh preview
commit-storage-tier:
	bash ./scripts/deliver-storage-tier.sh commit
push-storage-tier:
	bash ./scripts/deliver-storage-tier.sh push
EOF

awk '
{
	print
	if (!added && $0 ~ /^- \[ \] C042 /) {
		print "- [x] C042a Immutable age-threshold hot, warm, and cold tier policy over disk placement."
		added = 1
	}
}
END {
	if (!added) {
		print "missing C042 checklist row" > "/dev/stderr"
		exit 1
	}
}
' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
	STORAGE_TIERS.md \
	hat/hatStorage/storage_tier.go \
	hat/hatStorage/storage_tier_test.go \
	scripts/benchmark-storage-tier.sh \
	scripts/deliver-storage-tier.sh \
	scripts/format-storage-tier.sh \
	scripts/test-race-storage-tier.sh \
	scripts/test-storage-tier.sh

for generated in Makefile INSPIRATION.md; do
	blob=$(git hash-object -w "$tmpdir/$generated")
	GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

echo "storage-tier delivery mode: $mode"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
	exit 0
fi

if [[ "$mode" == commit ]]; then
	GIT_INDEX_FILE="$index" git commit -m "feat(storage): add age-based storage tiers"
	exit 0
fi

git push origin HEAD:master
