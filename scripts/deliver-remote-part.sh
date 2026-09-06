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

tmpdir="$root/.delivery-remote-part.$$"
index="$root/.git/remote-part-index.$$"
cleanup() {
	rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT

mkdir -p -- "$tmpdir"
git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"

cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-remote-part test-remote-part test-race-remote-part benchmark-remote-part deliver-remote-part commit-remote-part push-remote-part
format-remote-part:
	bash ./scripts/format-remote-part.sh
test-remote-part:
	bash ./scripts/test-remote-part.sh
test-race-remote-part:
	bash ./scripts/test-race-remote-part.sh
benchmark-remote-part:
	bash ./scripts/benchmark-remote-part.sh
deliver-remote-part:
	bash ./scripts/deliver-remote-part.sh preview
commit-remote-part:
	bash ./scripts/deliver-remote-part.sh commit
push-remote-part:
	bash ./scripts/deliver-remote-part.sh push
EOF

awk '
{
	print
	if (!added && $0 ~ /^- \[ \] C043 /) {
		print "- [x] C043a Validated immutable remote-part references with root-confined local metadata paths."
		added = 1
	}
}
END {
	if (!added) {
		print "missing C043 checklist row" > "/dev/stderr"
		exit 1
	}
}
' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
	REMOTE_PARTS.md \
	hat/hatStorage/remote_part.go \
	hat/hatStorage/remote_part_test.go \
	scripts/benchmark-remote-part.sh \
	scripts/deliver-remote-part.sh \
	scripts/format-remote-part.sh \
	scripts/test-race-remote-part.sh \
	scripts/test-remote-part.sh

for generated in Makefile INSPIRATION.md; do
	blob=$(git hash-object -w "$tmpdir/$generated")
	GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

echo "remote-part delivery mode: $mode"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
	exit 0
fi

if [[ "$mode" == commit ]]; then
	GIT_INDEX_FILE="$index" git commit -m "feat(storage): add remote part references"
	exit 0
fi

git push origin HEAD:master
