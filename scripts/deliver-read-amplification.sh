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

tmpdir="$root/.delivery-read-amplification.$$"
index="$root/.git/read-amplification-index.$$"
cleanup() {
	rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT

mkdir -p -- "$tmpdir"
git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"

cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-read-amplification test-read-amplification test-race-read-amplification benchmark-read-amplification deliver-read-amplification commit-read-amplification push-read-amplification
format-read-amplification:
	bash ./scripts/format-read-amplification.sh
test-read-amplification:
	bash ./scripts/test-read-amplification.sh
test-race-read-amplification:
	bash ./scripts/test-race-read-amplification.sh
benchmark-read-amplification:
	bash ./scripts/benchmark-read-amplification.sh
deliver-read-amplification:
	bash ./scripts/deliver-read-amplification.sh preview
commit-read-amplification:
	bash ./scripts/deliver-read-amplification.sh commit
push-read-amplification:
	bash ./scripts/deliver-read-amplification.sh push
EOF

awk '
{
	print
	if (!added && $0 ~ /^- \[ \] C046 /) {
		print "- [x] C046a Per-part and per-column read amplification accounting with deterministic snapshots."
		added = 1
	}
}
END {
	if (!added) {
		print "missing C046 checklist row" > "/dev/stderr"
		exit 1
	}
}
' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
	READ_AMPLIFICATION.md \
	hat/hatMetrics/read_amplification.go \
	hat/hatMetrics/read_amplification_test.go \
	scripts/benchmark-read-amplification.sh \
	scripts/deliver-read-amplification.sh \
	scripts/format-read-amplification.sh \
	scripts/test-read-amplification.sh \
	scripts/test-race-read-amplification.sh

for generated in Makefile INSPIRATION.md; do
	blob=$(git hash-object -w "$tmpdir/$generated")
	GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

echo "read-amplification delivery mode: $mode"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
	exit 0
fi

if [[ "$mode" == commit ]]; then
	GIT_INDEX_FILE="$index" git commit -m "feat(metrics): add read amplification accounting"
	exit 0
fi

git push origin HEAD:master
