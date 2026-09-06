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
tmpdir="$root/.delivery-codec-metrics.$$"
index="$root/.git/codec-metrics-index.$$"
cleanup() {
    rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT
mkdir -p -- "$tmpdir"

git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"
cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-codec-metrics
format-codec-metrics:
	bash ./scripts/format-codec-metrics.sh

.PHONY: test-codec-metrics
test-codec-metrics:
	bash ./scripts/test-codec-metrics.sh

.PHONY: test-race-codec-metrics
test-race-codec-metrics:
	bash ./scripts/test-race-codec-metrics.sh

.PHONY: benchmark-codec-metrics
benchmark-codec-metrics:
	bash ./scripts/benchmark-codec-metrics.sh

.PHONY: deliver-codec-metrics
deliver-codec-metrics:
	bash ./scripts/deliver-codec-metrics.sh preview

.PHONY: commit-codec-metrics
commit-codec-metrics:
	bash ./scripts/deliver-codec-metrics.sh commit

.PHONY: push-codec-metrics
push-codec-metrics:
	bash ./scripts/deliver-codec-metrics.sh push
EOF

awk '
    {
        print
        if (!added && $0 == "- [ ] C060 Compression ratio and decompression CPU accounting.") {
            print "- [x] C060a Atomic codec byte and CPU accounting with derived compression ratio."
            added = 1
        }
    }
    END {
        if (!added) {
            print "missing C060 checklist row" > "/dev/stderr"
            exit 1
        }
    }
' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv -- "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
    CODEC_METRICS.md \
    hat/hatMetrics/codec_metrics.go \
    hat/hatMetrics/codec_metrics_test.go \
    scripts/benchmark-codec-metrics.sh \
    scripts/deliver-codec-metrics.sh \
    scripts/format-codec-metrics.sh \
    scripts/test-codec-metrics.sh \
    scripts/test-race-codec-metrics.sh

for generated in Makefile INSPIRATION.md; do
    blob=$(git hash-object -w "$tmpdir/$generated")
    GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

printf 'codec-metrics delivery mode: %s\n' "$mode"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
    exit 0
fi

if [[ "$mode" == commit ]]; then
    GIT_INDEX_FILE="$index" git commit -m "feat(metrics): track codec cost and ratio"
    exit 0
fi

git push origin HEAD:master
