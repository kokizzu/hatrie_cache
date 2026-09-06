#!/usr/bin/env bash
set -euo pipefail

mode="${1:-preview}"
case "$mode" in
preview|commit|push) ;;
*)
    printf 'usage: %s [preview|commit|push]\n' "$0" >&2
    exit 2
    ;;
esac

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
tmpdir="$root/.delivery-nullable-bitmap.$$"
index="$root/.git/nullable-bitmap-index.$$"
cleanup() {
    rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT
mkdir -p -- "$tmpdir"

git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"
cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-nullable-bitmap
format-nullable-bitmap:
	bash ./scripts/format-nullable-bitmap.sh

.PHONY: test-nullable-bitmap
test-nullable-bitmap:
	bash ./scripts/test-nullable-bitmap.sh

.PHONY: test-race-nullable-bitmap
test-race-nullable-bitmap:
	bash ./scripts/test-race-nullable-bitmap.sh

.PHONY: benchmark-nullable-bitmap
benchmark-nullable-bitmap:
	bash ./scripts/benchmark-nullable-bitmap.sh

.PHONY: deliver-nullable-bitmap
deliver-nullable-bitmap:
	bash ./scripts/deliver-nullable-bitmap.sh preview

.PHONY: commit-nullable-bitmap
commit-nullable-bitmap:
	bash ./scripts/deliver-nullable-bitmap.sh commit

.PHONY: push-nullable-bitmap
push-nullable-bitmap:
	bash ./scripts/deliver-nullable-bitmap.sh push
EOF

awk '
    {
        print
        if (!added && $0 ~ /^- \[ \] C053 /) {
            print "- [x] C053a One-bit-per-row nullable bitmap with resize preservation and population counting."
            added = 1
        }
    }
    END {
        if (!added) {
            print "missing C053 checklist row" > "/dev/stderr"
            exit 1
        }
    }
' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv -- "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
    NULLABLE_BITMAP.md \
    hat/hatDataStructure/nullable_bitmap.go \
    hat/hatDataStructure/nullable_bitmap_test.go \
    scripts/benchmark-nullable-bitmap.sh \
    scripts/deliver-nullable-bitmap.sh \
    scripts/format-nullable-bitmap.sh \
    scripts/test-nullable-bitmap.sh \
    scripts/test-race-nullable-bitmap.sh

for generated in Makefile INSPIRATION.md; do
    blob=$(git hash-object -w "$tmpdir/$generated")
    GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

printf 'nullable-bitmap delivery mode: %s\n' "$mode"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
    exit 0
fi

if [[ "$mode" == commit ]]; then
    GIT_INDEX_FILE="$index" git commit -m "feat(data-structure): add nullable bitmap"
    exit 0
fi

git push origin HEAD:master
