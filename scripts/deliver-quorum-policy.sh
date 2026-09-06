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
tmpdir="$root/.delivery-quorum-policy.$$"
index="$root/.git/quorum-policy-index.$$"
cleanup() {
    rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT
mkdir -p -- "$tmpdir"

git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"
cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-quorum-policy
format-quorum-policy:
	bash ./scripts/format-quorum-policy.sh

.PHONY: test-quorum-policy
test-quorum-policy:
	bash ./scripts/test-quorum-policy.sh

.PHONY: test-race-quorum-policy
test-race-quorum-policy:
	bash ./scripts/test-race-quorum-policy.sh

.PHONY: benchmark-quorum-policy
benchmark-quorum-policy:
	bash ./scripts/benchmark-quorum-policy.sh

.PHONY: deliver-quorum-policy
deliver-quorum-policy:
	bash ./scripts/deliver-quorum-policy.sh preview

.PHONY: commit-quorum-policy
commit-quorum-policy:
	bash ./scripts/deliver-quorum-policy.sh commit

.PHONY: push-quorum-policy
push-quorum-policy:
	bash ./scripts/deliver-quorum-policy.sh push
EOF

awk '
    {
        print
        if (!added && $0 == "- [ ] T047 Synchronous replication with an explicit quorum.") {
            print "- [x] T047a Explicit write-quorum decision helper with validation and acknowledgement reporting."
            added = 1
        }
    }
    END {
        if (!added) {
            print "missing T047 checklist row" > "/dev/stderr"
            exit 1
        }
    }
' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv -- "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
    QUORUM_POLICY.md \
    hat/hatReplication/quorum_policy.go \
    hat/hatReplication/quorum_policy_test.go \
    scripts/benchmark-quorum-policy.sh \
    scripts/deliver-quorum-policy.sh \
    scripts/format-quorum-policy.sh \
    scripts/test-quorum-policy.sh \
    scripts/test-race-quorum-policy.sh

for generated in Makefile INSPIRATION.md; do
    blob=$(git hash-object -w "$tmpdir/$generated")
    GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

printf 'quorum-policy delivery mode: %s\n' "$mode"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
    exit 0
fi

if [[ "$mode" == commit ]]; then
    GIT_INDEX_FILE="$index" git commit -m "feat(replication): add write quorum policy"
    exit 0
fi

git push origin HEAD:master
