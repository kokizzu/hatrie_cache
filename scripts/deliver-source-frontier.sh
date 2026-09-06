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

tmpdir="$root/.delivery-source-frontier.$$"
index="$root/.git/source-frontier-index.$$"
cleanup() {
	rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT
mkdir -p -- "$tmpdir"

git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"
cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-source-frontier
format-source-frontier:
	bash ./scripts/format-source-frontier.sh

.PHONY: test-source-frontier
test-source-frontier:
	bash ./scripts/test-source-frontier.sh

.PHONY: test-race-source-frontier
test-race-source-frontier:
	bash ./scripts/test-race-source-frontier.sh

.PHONY: benchmark-source-frontier
benchmark-source-frontier:
	bash ./scripts/benchmark-source-frontier.sh

.PHONY: deliver-source-frontier
deliver-source-frontier:
	bash ./scripts/deliver-source-frontier.sh preview

.PHONY: commit-source-frontier
commit-source-frontier:
	bash ./scripts/deliver-source-frontier.sh commit

.PHONY: push-source-frontier
push-source-frontier:
	bash ./scripts/deliver-source-frontier.sh push
EOF
awk '
{
	print
	if (!added && $0 == "- [ ] M083 Per-source lag and frontier metrics.") {
		print "- [x] M083a Thread-safe monotone source frontier registry with deterministic lag snapshots."
		added = 1
	}
}
END {
	if (!added) {
		printf "M083 parent checklist row not found\n" > "/dev/stderr"
		exit 1
	}
}' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv -- "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
	SOURCE_FRONTIER.md \
	hat/hatMetrics/source_frontier.go \
	hat/hatMetrics/source_frontier_test.go \
	scripts/format-source-frontier.sh \
	scripts/test-source-frontier.sh \
	scripts/test-race-source-frontier.sh \
	scripts/benchmark-source-frontier.sh \
	scripts/deliver-source-frontier.sh

for generated in Makefile INSPIRATION.md; do
	blob=$(git hash-object -w "$tmpdir/$generated")
	GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

printf 'Isolated source-frontier change:\n'
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
	exit 0
fi
if [[ "$mode" == commit ]]; then
	GIT_INDEX_FILE="$index" git commit -m "feat(metrics): track source frontiers"
	exit 0
fi

git push origin HEAD:master
