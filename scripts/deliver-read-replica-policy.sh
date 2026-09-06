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

tmpdir="$root/.delivery-read-replica-policy.$$"
index="$root/.git/read-replica-policy-index.$$"
cleanup() {
	rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT
mkdir -p -- "$tmpdir"

git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"
cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-read-replica-policy
format-read-replica-policy:
	bash ./scripts/format-read-replica-policy.sh

.PHONY: test-read-replica-policy
test-read-replica-policy:
	bash ./scripts/test-read-replica-policy.sh

.PHONY: test-race-read-replica-policy
test-race-read-replica-policy:
	bash ./scripts/test-race-read-replica-policy.sh

.PHONY: benchmark-read-replica-policy
benchmark-read-replica-policy:
	bash ./scripts/benchmark-read-replica-policy.sh

.PHONY: deliver-read-replica-policy
deliver-read-replica-policy:
	bash ./scripts/deliver-read-replica-policy.sh preview

.PHONY: commit-read-replica-policy
commit-read-replica-policy:
	bash ./scripts/deliver-read-replica-policy.sh commit

.PHONY: push-read-replica-policy
push-read-replica-policy:
	bash ./scripts/deliver-read-replica-policy.sh push
EOF
awk '
{
	print
	if (!added && $0 == "- [ ] M088 Read replicas with explicit staleness bounds.") {
		print "- [x] M088a Deterministic read-replica selection with required frontiers and maximum lag."
		added = 1
	}
}
END {
	if (!added) {
		printf "M088 parent checklist row not found\n" > "/dev/stderr"
		exit 1
	}
}' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv -- "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
	READ_REPLICA_POLICY.md \
	hat/hatReplication/read_replica_policy.go \
	hat/hatReplication/read_replica_policy_test.go \
	scripts/benchmark-read-replica-policy.sh \
	scripts/deliver-read-replica-policy.sh \
	scripts/format-read-replica-policy.sh \
	scripts/test-read-replica-policy.sh \
	scripts/test-race-read-replica-policy.sh

for generated in Makefile INSPIRATION.md; do
	blob=$(git hash-object -w "$tmpdir/$generated")
	GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

printf 'Isolated read-replica policy change:\n'
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
	exit 0
fi
if [[ "$mode" == commit ]]; then
	GIT_INDEX_FILE="$index" git commit -m "feat(replication): bound read replica staleness"
	exit 0
fi

git push origin HEAD:master
