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
tmpdir="$root/.delivery-differential-multiset.$$"
index="$root/.git/differential-multiset-index.$$"
cleanup() {
    rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT
mkdir -p -- "$tmpdir"

git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"
cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-differential-multiset
format-differential-multiset:
	bash ./scripts/format-differential-multiset.sh

.PHONY: test-differential-multiset
test-differential-multiset:
	bash ./scripts/test-differential-multiset.sh

.PHONY: test-race-differential-multiset
test-race-differential-multiset:
	bash ./scripts/test-race-differential-multiset.sh

.PHONY: benchmark-differential-multiset
benchmark-differential-multiset:
	bash ./scripts/benchmark-differential-multiset.sh

.PHONY: deliver-differential-multiset
deliver-differential-multiset:
	bash ./scripts/deliver-differential-multiset.sh preview

.PHONY: commit-differential-multiset
commit-differential-multiset:
	bash ./scripts/deliver-differential-multiset.sh commit

.PHONY: push-differential-multiset
push-differential-multiset:
	bash ./scripts/deliver-differential-multiset.sh push
EOF

awk '
    {
        print
        if (!added && $0 ~ /^- \[ \] M002 /) {
            print "- [x] M002a Generic differential multiset keyed by comparable data and time with zero-consolidation and overflow checks."
            added = 1
        }
    }
    END {
        if (!added) {
            print "missing M002 checklist row" > "/dev/stderr"
            exit 1
        }
    }
' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv -- "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
    DIFFERENTIAL_MULTISET.md \
    hat/hatDataStructure/differential_multiset.go \
    hat/hatDataStructure/differential_multiset_test.go \
    scripts/benchmark-differential-multiset.sh \
    scripts/deliver-differential-multiset.sh \
    scripts/format-differential-multiset.sh \
    scripts/test-differential-multiset.sh \
    scripts/test-race-differential-multiset.sh

for generated in Makefile INSPIRATION.md; do
    blob=$(git hash-object -w "$tmpdir/$generated")
    GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

printf 'differential-multiset delivery mode: %s\n' "$mode"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
    exit 0
fi

if [[ "$mode" == commit ]]; then
    GIT_INDEX_FILE="$index" git commit -m "feat(data-structure): add differential multiset"
    exit 0
fi

git push origin HEAD:master
