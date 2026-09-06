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

tmpdir="$root/.delivery-operator-memory.$$"
index="$root/.git/operator-memory-index.$$"
cleanup() {
	rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT
mkdir -p -- "$tmpdir"

git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"
cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-operator-memory
format-operator-memory:
	bash ./scripts/format-operator-memory.sh

.PHONY: test-operator-memory
test-operator-memory:
	bash ./scripts/test-operator-memory.sh

.PHONY: test-race-operator-memory
test-race-operator-memory:
	bash ./scripts/test-race-operator-memory.sh

.PHONY: benchmark-operator-memory
benchmark-operator-memory:
	bash ./scripts/benchmark-operator-memory.sh

.PHONY: deliver-operator-memory
deliver-operator-memory:
	bash ./scripts/deliver-operator-memory.sh preview

.PHONY: commit-operator-memory
commit-operator-memory:
	bash ./scripts/deliver-operator-memory.sh commit

.PHONY: push-operator-memory
push-operator-memory:
	bash ./scripts/deliver-operator-memory.sh push
EOF
awk '
{
	print
	if (!added && $0 == "- [ ] M084 Per-operator retained-memory metrics.") {
		print "- [x] M084a Thread-safe operator retained-memory gauge registry with deterministic snapshots."
		added = 1
	}
}
END {
	if (!added) {
		printf "M084 parent checklist row not found\n" > "/dev/stderr"
		exit 1
	}
}' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv -- "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
	OPERATOR_MEMORY.md \
	hat/hatMetrics/operator_memory.go \
	hat/hatMetrics/operator_memory_test.go \
	scripts/benchmark-operator-memory.sh \
	scripts/deliver-operator-memory.sh \
	scripts/format-operator-memory.sh \
	scripts/test-operator-memory.sh \
	scripts/test-race-operator-memory.sh

for generated in Makefile INSPIRATION.md; do
	blob=$(git hash-object -w "$tmpdir/$generated")
	GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

printf 'Isolated operator-memory change:\n'
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
	exit 0
fi
if [[ "$mode" == commit ]]; then
	GIT_INDEX_FILE="$index" git commit -m "feat(metrics): track operator memory"
	exit 0
fi

git push origin HEAD:master
