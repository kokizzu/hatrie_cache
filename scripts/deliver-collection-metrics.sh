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

tmpdir="$root/.delivery-collection-metrics.$$"
index="$root/.git/collection-metrics-index.$$"
cleanup() {
	rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT
mkdir -p -- "$tmpdir"

git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"
cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-collection-metrics
format-collection-metrics:
	bash ./scripts/format-collection-metrics.sh

.PHONY: test-collection-metrics
test-collection-metrics:
	bash ./scripts/test-collection-metrics.sh

.PHONY: test-race-collection-metrics
test-race-collection-metrics:
	bash ./scripts/test-race-collection-metrics.sh

.PHONY: benchmark-collection-metrics
benchmark-collection-metrics:
	bash ./scripts/benchmark-collection-metrics.sh

.PHONY: deliver-collection-metrics
deliver-collection-metrics:
	bash ./scripts/deliver-collection-metrics.sh preview

.PHONY: commit-collection-metrics
commit-collection-metrics:
	bash ./scripts/deliver-collection-metrics.sh commit

.PHONY: push-collection-metrics
push-collection-metrics:
	bash ./scripts/deliver-collection-metrics.sh push
EOF
awk '
{
	print
	if (!added && $0 == "- [ ] M085 Per-collection size and compaction metrics.") {
		print "- [x] M085a Thread-safe collection size gauges and compaction counters with deterministic snapshots."
		added = 1
	}
}
END {
	if (!added) {
		printf "M085 parent checklist row not found\n" > "/dev/stderr"
		exit 1
	}
}' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv -- "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
	COLLECTION_METRICS.md \
	hat/hatMetrics/collection_metrics.go \
	hat/hatMetrics/collection_metrics_test.go \
	scripts/benchmark-collection-metrics.sh \
	scripts/deliver-collection-metrics.sh \
	scripts/format-collection-metrics.sh \
	scripts/test-collection-metrics.sh \
	scripts/test-race-collection-metrics.sh

for generated in Makefile INSPIRATION.md; do
	blob=$(git hash-object -w "$tmpdir/$generated")
	GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

printf 'Isolated collection-metrics change:\n'
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
	exit 0
fi
if [[ "$mode" == commit ]]; then
	GIT_INDEX_FILE="$index" git commit -m "feat(metrics): track collection sizes"
	exit 0
fi

git push origin HEAD:master
