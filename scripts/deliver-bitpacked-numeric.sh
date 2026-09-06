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
tmpdir="$root/.delivery-bitpacked-numeric.$$"
index="$root/.git/bitpacked-numeric-index.$$"
cleanup() {
    rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT
mkdir -p -- "$tmpdir"

git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"
cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-bitpacked-numeric
format-bitpacked-numeric:
	bash ./scripts/format-bitpacked-numeric.sh

.PHONY: test-bitpacked-numeric
test-bitpacked-numeric:
	bash ./scripts/test-bitpacked-numeric.sh

.PHONY: test-race-bitpacked-numeric
test-race-bitpacked-numeric:
	bash ./scripts/test-race-bitpacked-numeric.sh

.PHONY: benchmark-bitpacked-numeric
benchmark-bitpacked-numeric:
	bash ./scripts/benchmark-bitpacked-numeric.sh

.PHONY: deliver-bitpacked-numeric
deliver-bitpacked-numeric:
	bash ./scripts/deliver-bitpacked-numeric.sh preview

.PHONY: commit-bitpacked-numeric
commit-bitpacked-numeric:
	bash ./scripts/deliver-bitpacked-numeric.sh commit

.PHONY: push-bitpacked-numeric
push-bitpacked-numeric:
	bash ./scripts/deliver-bitpacked-numeric.sh push
EOF

awk '
    {
        print
        if (!added && $0 ~ /^- \[ \] C048 /) {
            print "- [x] C048a Minimum-width bit-packed uint64 codec selected from column maximum with canonical validation."
            added = 1
        }
    }
    END {
        if (!added) {
            print "missing C048 checklist row" > "/dev/stderr"
            exit 1
        }
    }
' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv -- "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
    BITPACKED_NUMERIC.md \
    hat/hatCodec/bitpacked_numeric.go \
    hat/hatCodec/bitpacked_numeric_test.go \
    scripts/benchmark-bitpacked-numeric.sh \
    scripts/deliver-bitpacked-numeric.sh \
    scripts/format-bitpacked-numeric.sh \
    scripts/test-bitpacked-numeric.sh \
    scripts/test-race-bitpacked-numeric.sh

for generated in Makefile INSPIRATION.md; do
    blob=$(git hash-object -w "$tmpdir/$generated")
    GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

printf 'bitpacked-numeric delivery mode: %s\n' "$mode"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
    exit 0
fi

if [[ "$mode" == commit ]]; then
    GIT_INDEX_FILE="$index" git commit -m "feat(codec): add bit-packed numeric encoding"
    exit 0
fi

git push origin HEAD:master
