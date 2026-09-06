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

tmpdir="$root/.delivery-compression-negotiation.$$"
index="$root/.git/compression-negotiation-index.$$"
cleanup() {
	rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT

mkdir -p -- "$tmpdir"
git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"

cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-compression-negotiation test-compression-negotiation test-race-compression-negotiation benchmark-compression-negotiation deliver-compression-negotiation commit-compression-negotiation push-compression-negotiation
format-compression-negotiation:
	bash ./scripts/format-compression-negotiation.sh
test-compression-negotiation:
	bash ./scripts/test-compression-negotiation.sh
test-race-compression-negotiation:
	bash ./scripts/test-race-compression-negotiation.sh
benchmark-compression-negotiation:
	bash ./scripts/benchmark-compression-negotiation.sh
deliver-compression-negotiation:
	bash ./scripts/deliver-compression-negotiation.sh preview
commit-compression-negotiation:
	bash ./scripts/deliver-compression-negotiation.sh commit
push-compression-negotiation:
	bash ./scripts/deliver-compression-negotiation.sh push
EOF

awk '
{
	print
	if (!added && $0 ~ /^- \[ \] C123 /) {
		print "- [x] C123a Compatible per-client compression level negotiation with explicit range intersection."
		added = 1
	}
}
END {
	if (!added) {
		print "missing C123 checklist row" > "/dev/stderr"
		exit 1
	}
}
' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
	COMPRESSION_NEGOTIATION.md \
	hat/hatCodec/compression_negotiation.go \
	hat/hatCodec/compression_negotiation_test.go \
	scripts/benchmark-compression-negotiation.sh \
	scripts/deliver-compression-negotiation.sh \
	scripts/format-compression-negotiation.sh \
	scripts/test-race-compression-negotiation.sh \
	scripts/test-compression-negotiation.sh

for generated in Makefile INSPIRATION.md; do
	blob=$(git hash-object -w "$tmpdir/$generated")
	GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

echo "compression-negotiation delivery mode: $mode"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
	exit 0
fi

if [[ "$mode" == commit ]]; then
	GIT_INDEX_FILE="$index" git commit -m "feat(codec): add compression level negotiation"
	exit 0
fi

git push origin HEAD:master
