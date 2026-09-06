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
tmpdir="$root/.delivery-conflict-resolution.$$"
index="$root/.git/conflict-resolution-index.$$"
cleanup() {
    rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT
mkdir -p -- "$tmpdir"

git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"
cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-conflict-resolution
format-conflict-resolution:
	bash ./scripts/format-conflict-resolution.sh

.PHONY: test-conflict-resolution
test-conflict-resolution:
	bash ./scripts/test-conflict-resolution.sh

.PHONY: test-race-conflict-resolution
test-race-conflict-resolution:
	bash ./scripts/test-race-conflict-resolution.sh

.PHONY: benchmark-conflict-resolution
benchmark-conflict-resolution:
	bash ./scripts/benchmark-conflict-resolution.sh

.PHONY: deliver-conflict-resolution
deliver-conflict-resolution:
	bash ./scripts/deliver-conflict-resolution.sh preview

.PHONY: commit-conflict-resolution
commit-conflict-resolution:
	bash ./scripts/deliver-conflict-resolution.sh commit

.PHONY: push-conflict-resolution
push-conflict-resolution:
	bash ./scripts/deliver-conflict-resolution.sh push
EOF

awk '
    {
        print
        if (!added && $0 == "- [ ] T056 Deterministic conflict resolution for concurrent writers.") {
            print "- [x] T056a Deterministic conflict-version ordering with stable node and sequence tie-breaks."
            added = 1
        }
    }
    END {
        if (!added) {
            print "missing T056 checklist row" > "/dev/stderr"
            exit 1
        }
    }
' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv -- "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
    CONFLICT_RESOLUTION.md \
    hat/hatReplication/conflict_resolution.go \
    hat/hatReplication/conflict_resolution_test.go \
    scripts/benchmark-conflict-resolution.sh \
    scripts/deliver-conflict-resolution.sh \
    scripts/format-conflict-resolution.sh \
    scripts/test-conflict-resolution.sh \
    scripts/test-race-conflict-resolution.sh

for generated in Makefile INSPIRATION.md; do
    blob=$(git hash-object -w "$tmpdir/$generated")
    GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

printf 'conflict-resolution delivery mode: %s\n' "$mode"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
    exit 0
fi

if [[ "$mode" == commit ]]; then
    GIT_INDEX_FILE="$index" git commit -m "feat(replication): add deterministic conflict resolution"
    exit 0
fi

git push origin HEAD:master
