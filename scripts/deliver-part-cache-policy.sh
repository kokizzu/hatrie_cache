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

tmpdir="$root/.delivery-part-cache-policy.$$"
index="$root/.git/part-cache-policy-index.$$"
cleanup() {
	rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT

mkdir -p -- "$tmpdir"
git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"

cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-part-cache-policy test-part-cache-policy test-race-part-cache-policy benchmark-part-cache-policy deliver-part-cache-policy commit-part-cache-policy push-part-cache-policy
format-part-cache-policy:
	bash ./scripts/format-part-cache-policy.sh
test-part-cache-policy:
	bash ./scripts/test-part-cache-policy.sh
test-race-part-cache-policy:
	bash ./scripts/test-race-part-cache-policy.sh
benchmark-part-cache-policy:
	bash ./scripts/benchmark-part-cache-policy.sh
deliver-part-cache-policy:
	bash ./scripts/deliver-part-cache-policy.sh preview
commit-part-cache-policy:
	bash ./scripts/deliver-part-cache-policy.sh commit
push-part-cache-policy:
	bash ./scripts/deliver-part-cache-policy.sh push
EOF

awk '
{
	print
	if (!added && $0 ~ /^- \[ \] C045 /) {
		print "- [x] C045a Explicit part-cache admission and deterministic LFU/LRU eviction planning."
		added = 1
	}
}
END {
	if (!added) {
		print "missing C045 checklist row" > "/dev/stderr"
		exit 1
	}
}
' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
	PART_CACHE_POLICY.md \
	hat/hatStorage/part_cache_policy.go \
	hat/hatStorage/part_cache_policy_test.go \
	scripts/benchmark-part-cache-policy.sh \
	scripts/deliver-part-cache-policy.sh \
	scripts/format-part-cache-policy.sh \
	scripts/test-race-part-cache-policy.sh \
	scripts/test-part-cache-policy.sh

for generated in Makefile INSPIRATION.md; do
	blob=$(git hash-object -w "$tmpdir/$generated")
	GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

echo "part-cache-policy delivery mode: $mode"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
	exit 0
fi

if [[ "$mode" == commit ]]; then
	GIT_INDEX_FILE="$index" git commit -m "feat(storage): add part cache policy"
	exit 0
fi

git push origin HEAD:master
