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
tmpdir="$root/.delivery-gorilla-float.$$"
index="$root/.git/gorilla-float-index.$$"
cleanup() {
    rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT
mkdir -p -- "$tmpdir"

git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"
cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-gorilla-float
format-gorilla-float:
	bash ./scripts/format-gorilla-float.sh

.PHONY: test-gorilla-float
test-gorilla-float:
	bash ./scripts/test-gorilla-float.sh

.PHONY: test-race-gorilla-float
test-race-gorilla-float:
	bash ./scripts/test-race-gorilla-float.sh

.PHONY: benchmark-gorilla-float
benchmark-gorilla-float:
	bash ./scripts/benchmark-gorilla-float.sh

.PHONY: deliver-gorilla-float
deliver-gorilla-float:
	bash ./scripts/deliver-gorilla-float.sh preview

.PHONY: commit-gorilla-float
commit-gorilla-float:
	bash ./scripts/deliver-gorilla-float.sh commit

.PHONY: push-gorilla-float
push-gorilla-float:
	bash ./scripts/deliver-gorilla-float.sh push
EOF

awk '
    {
        print
        if (!added && $0 ~ /^- \[ \] C057 /) {
            print "- [x] C057a Bit-preserving XOR window codec for repeated and slowly changing float64 values."
            added = 1
        }
    }
    END {
        if (!added) {
            print "missing C057 checklist row" > "/dev/stderr"
            exit 1
        }
    }
' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv -- "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
    GORILLA_FLOAT.md \
    hat/hatCodec/gorilla_float.go \
    hat/hatCodec/gorilla_float_test.go \
    scripts/benchmark-gorilla-float.sh \
    scripts/deliver-gorilla-float.sh \
    scripts/format-gorilla-float.sh \
    scripts/test-gorilla-float.sh \
    scripts/test-race-gorilla-float.sh

for generated in Makefile INSPIRATION.md; do
    blob=$(git hash-object -w "$tmpdir/$generated")
    GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

printf 'gorilla-float delivery mode: %s\n' "$mode"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
    exit 0
fi

if [[ "$mode" == commit ]]; then
    GIT_INDEX_FILE="$index" git commit -m "feat(codec): add gorilla float encoding"
    exit 0
fi

git push origin HEAD:master
