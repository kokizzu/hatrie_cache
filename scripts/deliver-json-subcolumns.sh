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

tmpdir="$root/.delivery-json-subcolumns.$$"
index="$root/.git/json-subcolumns-index.$$"
cleanup() {
	rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT

mkdir -p -- "$tmpdir"
git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"

cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-json-subcolumns test-json-subcolumns test-race-json-subcolumns benchmark-json-subcolumns deliver-json-subcolumns commit-json-subcolumns push-json-subcolumns
format-json-subcolumns:
	bash ./scripts/format-json-subcolumns.sh
test-json-subcolumns:
	bash ./scripts/test-json-subcolumns.sh
test-race-json-subcolumns:
	bash ./scripts/test-race-json-subcolumns.sh
benchmark-json-subcolumns:
	bash ./scripts/benchmark-json-subcolumns.sh
deliver-json-subcolumns:
	bash ./scripts/deliver-json-subcolumns.sh preview
commit-json-subcolumns:
	bash ./scripts/deliver-json-subcolumns.sh commit
push-json-subcolumns:
	bash ./scripts/deliver-json-subcolumns.sh push
EOF

awk '
{
	print
	if (!added && $0 ~ /^- \[ \] C050 /) {
		print "- [x] C050a Process-local shared JSON subcolumn path interning with deterministic snapshots."
		added = 1
	}
}
END {
	if (!added) {
		print "missing C050 checklist row" > "/dev/stderr"
		exit 1
	}
}
' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
	JSON_SUBCOLUMNS.md \
	hat/hatSql/json_subcolumns.go \
	hat/hatSql/json_subcolumns_test.go \
	scripts/benchmark-json-subcolumns.sh \
	scripts/deliver-json-subcolumns.sh \
	scripts/format-json-subcolumns.sh \
	scripts/test-race-json-subcolumns.sh \
	scripts/test-json-subcolumns.sh

for generated in Makefile INSPIRATION.md; do
	blob=$(git hash-object -w "$tmpdir/$generated")
	GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

echo "json-subcolumns delivery mode: $mode"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
	exit 0
fi

if [[ "$mode" == commit ]]; then
	GIT_INDEX_FILE="$index" git commit -m "feat(sql): add shared JSON subcolumn paths"
	exit 0
fi

git push origin HEAD:master
