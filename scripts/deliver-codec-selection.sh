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
tmpdir="$root/.delivery-codec-selection.$$"
index="$root/.git/codec-selection-index.$$"
cleanup() {
    rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT
mkdir -p -- "$tmpdir"

git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"
cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-codec-selection
format-codec-selection:
	bash ./scripts/format-codec-selection.sh

.PHONY: test-codec-selection
test-codec-selection:
	bash ./scripts/test-codec-selection.sh

.PHONY: test-race-codec-selection
test-race-codec-selection:
	bash ./scripts/test-race-codec-selection.sh

.PHONY: benchmark-codec-selection
benchmark-codec-selection:
	bash ./scripts/benchmark-codec-selection.sh

.PHONY: deliver-codec-selection
deliver-codec-selection:
	bash ./scripts/deliver-codec-selection.sh preview

.PHONY: commit-codec-selection
commit-codec-selection:
	bash ./scripts/deliver-codec-selection.sh commit

.PHONY: push-codec-selection
push-codec-selection:
	bash ./scripts/deliver-codec-selection.sh push
EOF

awk '
    {
        print
        if (!added && $0 ~ /^- \[ \] C059 /) {
            print "- [x] C059a Stackless byte-entropy estimator with conservative raw-or-compressed codec recommendation."
            added = 1
        }
    }
    END {
        if (!added) {
            print "missing C059 checklist row" > "/dev/stderr"
            exit 1
        }
    }
' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv -- "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
    CODEC_SELECTION.md \
    hat/hatCodec/codec_selection.go \
    hat/hatCodec/codec_selection_test.go \
    scripts/benchmark-codec-selection.sh \
    scripts/deliver-codec-selection.sh \
    scripts/format-codec-selection.sh \
    scripts/test-codec-selection.sh \
    scripts/test-race-codec-selection.sh

for generated in Makefile INSPIRATION.md; do
    blob=$(git hash-object -w "$tmpdir/$generated")
    GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

printf 'codec-selection delivery mode: %s\n' "$mode"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
    exit 0
fi

if [[ "$mode" == commit ]]; then
    GIT_INDEX_FILE="$index" git commit -m "feat(codec): add entropy codec selection"
    exit 0
fi

git push origin HEAD:master
