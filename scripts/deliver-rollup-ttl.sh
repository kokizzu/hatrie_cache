#!/usr/bin/env bash
set -euo pipefail

mode=${1:-preview}
repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-rollup-ttl-delivery.XXXXXX")
trap 'rm -rf -- "$tmpdir"' EXIT
index="$tmpdir/index"

if [[ "$mode" == "push" ]]; then
	git push
	git rev-parse HEAD
	exit 0
fi

if [[ "$mode" != "preview" && "$mode" != "commit" ]]; then
	echo "usage: $0 [preview|commit|push]" >&2
	exit 2
fi

target_block=$(cat <<'EOF'
# rollup-ttl-targets
format-rollup-ttl:
	bash ./scripts/format-rollup-ttl.sh

test-rollup-ttl:
	bash ./scripts/test-rollup-ttl.sh

test-race-rollup-ttl:
	bash ./scripts/test-race-rollup-ttl.sh

benchmark-rollup-ttl:
	bash ./scripts/benchmark-rollup-ttl.sh

deliver-rollup-ttl:
	bash ./scripts/deliver-rollup-ttl.sh preview

commit-rollup-ttl:
	bash ./scripts/deliver-rollup-ttl.sh commit

push-rollup-ttl:
	bash ./scripts/deliver-rollup-ttl.sh push
EOF
)
base_makefile="$tmpdir/Makefile"
git show HEAD:Makefile > "$base_makefile"
if ! grep -Fq -- '# rollup-ttl-targets' "$base_makefile"; then
	printf '\n%s\n' "$target_block" >> "$base_makefile"
fi

base_inspiration="$tmpdir/INSPIRATION.md"
git show HEAD:INSPIRATION.md > "$base_inspiration"
child='- [x] C038a TTL pruning removes only complete rollup buckets at explicit boundaries.'
if ! grep -Fq -- "$child" "$base_inspiration"; then
	printf '\n%s\n' "$child" >> "$base_inspiration"
fi

GIT_INDEX_FILE="$index" git read-tree HEAD
makefile_blob=$(git hash-object -w "$base_makefile")
inspiration_blob=$(git hash-object -w "$base_inspiration")
GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$inspiration_blob,INSPIRATION.md"

base_rollup="$tmpdir/rollup.go"
target_rollup="$tmpdir/rollup.target.go"
git show HEAD:hat/hatSql/rollup.go > "$base_rollup"
replacement="$tmpdir/rollup-ttl-method.txt"
cat > "$replacement" <<'EOF'
// ExpireBefore removes only buckets whose end is at or before cutoff. The
// cutoff must be a bucket boundary, so a current partial bucket is retained.
// Callers can implement TTL retention by passing now minus their retention
// duration; no background goroutine or wall-clock dependency is introduced.
func (rollup *TimeBucketRollup) ExpireBefore(cutoff time.Time) (int, error) {
	if rollup == nil {
		return 0, fmt.Errorf("time bucket rollup is nil")
	}
	if !cutoff.Equal(rollup.bucketStart(cutoff)) {
		return 0, fmt.Errorf("rollup expiration cutoff must be a bucket boundary")
	}
	rollup.mu.Lock()
	defer rollup.mu.Unlock()
	removed := 0
	for key, bucket := range rollup.buckets {
		if !bucket.End.After(cutoff) {
			delete(rollup.buckets, key)
			removed++
		}
	}
	return removed, nil
}
EOF
awk -v replacement="$replacement" '
    /^\/\/ RetainRawAfterVerified returns / {
        while ((getline line < replacement) > 0) print line
        close(replacement)
        print ""
    }
    { print }
' "$base_rollup" > "$target_rollup"
if ! grep -Fq -- 'func (rollup *TimeBucketRollup) ExpireBefore' "$target_rollup"; then
	echo 'failed to build the rollup TTL source' >&2
	exit 1
fi
rollup_blob=$(git hash-object -w "$target_rollup")
GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$rollup_blob,hat/hatSql/rollup.go"

GIT_INDEX_FILE="$index" git add -- \
	ROLLUP_TTL.md \
	hat/hatSql/rollup_ttl_test.go \
	scripts/benchmark-rollup-ttl.sh \
	scripts/deliver-rollup-ttl.sh \
	scripts/format-rollup-ttl.sh \
	scripts/test-rollup-ttl.sh \
	scripts/test-race-rollup-ttl.sh

GIT_INDEX_FILE="$index" git diff --cached --check
if GIT_INDEX_FILE="$index" git diff --cached --quiet; then
	echo "rollup TTL already delivered"
	git rev-parse HEAD
	exit 0
fi

echo "rollup TTL delivery ($mode)"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == "commit" ]]; then
	GIT_INDEX_FILE="$index" git commit -m "feat(sql): add explicit rollup TTL pruning"
	git rev-parse HEAD
fi
