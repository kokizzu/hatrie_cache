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

tmpdir="$root/.delivery-granule-sizing.$$"
index="$root/.git/granule-sizing-index.$$"
cleanup() {
	rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT

mkdir -p -- "$tmpdir"
git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"

cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-granule-sizing test-granule-sizing test-race-granule-sizing benchmark-granule-sizing deliver-granule-sizing commit-granule-sizing push-granule-sizing
format-granule-sizing:
	bash ./scripts/format-granule-sizing.sh
test-granule-sizing:
	bash ./scripts/test-granule-sizing.sh
test-race-granule-sizing:
	bash ./scripts/test-race-granule-sizing.sh
benchmark-granule-sizing:
	bash ./scripts/benchmark-granule-sizing.sh
deliver-granule-sizing:
	bash ./scripts/deliver-granule-sizing.sh preview
commit-granule-sizing:
	bash ./scripts/deliver-granule-sizing.sh commit
push-granule-sizing:
	bash ./scripts/deliver-granule-sizing.sh push
EOF

awk '
{
	print
	if (!added && $0 ~ /^- \[ \] C047 /) {
		print "- [x] C047a Bounded adaptive granule sizing from observed predicate selectivity."
		added = 1
	}
}
END {
	if (!added) {
		print "missing C047 checklist row" > "/dev/stderr"
		exit 1
	}
}
' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
	GRANULE_SIZING.md \
	hat/hatSql/granule_sizing.go \
	hat/hatSql/granule_sizing_test.go \
	scripts/benchmark-granule-sizing.sh \
	scripts/deliver-granule-sizing.sh \
	scripts/format-granule-sizing.sh \
	scripts/test-granule-sizing.sh \
	scripts/test-race-granule-sizing.sh

for generated in Makefile INSPIRATION.md; do
	blob=$(git hash-object -w "$tmpdir/$generated")
	GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

echo "granule-sizing delivery mode: $mode"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
	exit 0
fi

if [[ "$mode" == commit ]]; then
	GIT_INDEX_FILE="$index" git commit -m "feat(sql): add adaptive granule sizing policy"
	exit 0
fi

git push origin HEAD:master
