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
tmpdir="$root/.delivery-late-data-policy.$$"
index="$root/.git/late-data-policy-index.$$"
cleanup() {
    rm -rf -- "$tmpdir" "$index"
}
trap cleanup EXIT
mkdir -p -- "$tmpdir"

git show HEAD:Makefile > "$tmpdir/Makefile"
git show HEAD:INSPIRATION.md > "$tmpdir/INSPIRATION.md"
cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: format-late-data-policy
format-late-data-policy:
	bash ./scripts/format-late-data-policy.sh

.PHONY: test-late-data-policy
test-late-data-policy:
	bash ./scripts/test-late-data-policy.sh

.PHONY: test-race-late-data-policy
test-race-late-data-policy:
	bash ./scripts/test-race-late-data-policy.sh

.PHONY: benchmark-late-data-policy
benchmark-late-data-policy:
	bash ./scripts/benchmark-late-data-policy.sh

.PHONY: deliver-late-data-policy
deliver-late-data-policy:
	bash ./scripts/deliver-late-data-policy.sh preview

.PHONY: commit-late-data-policy
commit-late-data-policy:
	bash ./scripts/deliver-late-data-policy.sh commit

.PHONY: push-late-data-policy
push-late-data-policy:
	bash ./scripts/deliver-late-data-policy.sh push
EOF

awk '
    {
        print
        if (!added && $0 ~ /^- \[ \] M071 /) {
            print "- [x] M071a Bounded-lateness classifier with explicit drop-or-retain policy and boundary semantics."
            added = 1
        }
    }
    END {
        if (!added) {
            print "missing M071 checklist row" > "/dev/stderr"
            exit 1
        }
    }
' "$tmpdir/INSPIRATION.md" > "$tmpdir/INSPIRATION.md.next"
mv -- "$tmpdir/INSPIRATION.md.next" "$tmpdir/INSPIRATION.md"

GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -- \
    LATE_DATA_POLICY.md \
    hat/hatSql/late_data_policy.go \
    hat/hatSql/late_data_policy_test.go \
    scripts/benchmark-late-data-policy.sh \
    scripts/deliver-late-data-policy.sh \
    scripts/format-late-data-policy.sh \
    scripts/test-late-data-policy.sh \
    scripts/test-race-late-data-policy.sh

for generated in Makefile INSPIRATION.md; do
    blob=$(git hash-object -w "$tmpdir/$generated")
    GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$blob,$generated"
done

printf 'late-data-policy delivery mode: %s\n' "$mode"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == preview ]]; then
    exit 0
fi

if [[ "$mode" == commit ]]; then
    GIT_INDEX_FILE="$index" git commit -m "feat(sql): add late data policy"
    exit 0
fi

git push origin HEAD:master
