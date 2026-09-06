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

tmpdir="$root/.delivery-replay-digest.$$"
index="$root/.git/replay-digest-index.$$"
cleanup() {
	rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT
mkdir -p -- "$tmpdir"

git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"
cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-replay-digest
format-replay-digest:
	bash ./scripts/format-replay-digest.sh

.PHONY: test-replay-digest
test-replay-digest:
	bash ./scripts/test-replay-digest.sh

.PHONY: test-race-replay-digest
test-race-replay-digest:
	bash ./scripts/test-race-replay-digest.sh

.PHONY: benchmark-replay-digest
benchmark-replay-digest:
	bash ./scripts/benchmark-replay-digest.sh

.PHONY: deliver-replay-digest
deliver-replay-digest:
	bash ./scripts/deliver-replay-digest.sh preview

.PHONY: commit-replay-digest
commit-replay-digest:
	bash ./scripts/deliver-replay-digest.sh commit

.PHONY: push-replay-digest
push-replay-digest:
	bash ./scripts/deliver-replay-digest.sh push
EOF
awk '
{
	print
	if (!added && $0 == "- [ ] M087 Deterministic replica replay checks.") {
		print "- [x] M087a Canonical ordered replay digests with sequence validation and deterministic mismatch errors."
		added = 1
	}
}
END {
	if (!added) {
		printf "M087 parent checklist row not found\n" > "/dev/stderr"
		exit 1
	}
}' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv -- "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
	REPLAY_DIGEST.md \
	hat/hatReplication/replay_digest.go \
	hat/hatReplication/replay_digest_test.go \
	scripts/benchmark-replay-digest.sh \
	scripts/deliver-replay-digest.sh \
	scripts/format-replay-digest.sh \
	scripts/test-race-replay-digest.sh \
	scripts/test-replay-digest.sh

for generated in Makefile INSPIRATION.md; do
	blob=$(git hash-object -w "$tmpdir/$generated")
	GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

printf 'Isolated replay-digest change:\n'
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
	exit 0
fi
if [[ "$mode" == commit ]]; then
	GIT_INDEX_FILE="$index" git commit -m "feat(replication): verify replay digests"
	exit 0
fi

git push origin HEAD:master
