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
tmpdir="$root/.delivery-watermark.$$"
index="$root/.git/watermark-index.$$"
cleanup() {
    rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT
mkdir -p -- "$tmpdir"

git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"
cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-watermark
format-watermark:
	bash ./scripts/format-watermark.sh

.PHONY: test-watermark
test-watermark:
	bash ./scripts/test-watermark.sh

.PHONY: test-race-watermark
test-race-watermark:
	bash ./scripts/test-race-watermark.sh

.PHONY: benchmark-watermark
benchmark-watermark:
	bash ./scripts/benchmark-watermark.sh

.PHONY: deliver-watermark
deliver-watermark:
	bash ./scripts/deliver-watermark.sh preview

.PHONY: commit-watermark
commit-watermark:
	bash ./scripts/deliver-watermark.sh commit

.PHONY: push-watermark
push-watermark:
	bash ./scripts/deliver-watermark.sh push
EOF

awk '
    {
        print
        if (!added && $0 ~ /^- \[ \] M072 /) {
            print "- [x] M072a Safe minimum-source watermark merge with monotonic publication."
            added = 1
        }
    }
    END {
        if (!added) {
            print "missing M072 checklist row" > "/dev/stderr"
            exit 1
        }
    }
' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv -- "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
    WATERMARK.md \
    hat/hatSql/watermark.go \
    hat/hatSql/watermark_test.go \
    scripts/benchmark-watermark.sh \
    scripts/deliver-watermark.sh \
    scripts/format-watermark.sh \
    scripts/test-race-watermark.sh \
    scripts/test-watermark.sh

for generated in Makefile INSPIRATION.md; do
    blob=$(git hash-object -w "$tmpdir/$generated")
    GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

printf 'watermark delivery mode: %s\n' "$mode"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
    exit 0
fi

if [[ "$mode" == commit ]]; then
    GIT_INDEX_FILE="$index" git commit -m "feat(sql): add watermark propagation"
    exit 0
fi

git push origin HEAD:master
