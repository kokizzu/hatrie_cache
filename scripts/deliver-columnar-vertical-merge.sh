#!/usr/bin/env bash
set -euo pipefail

mode=${1:-preview}
repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-columnar-vertical-delivery.XXXXXX")
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
# columnar-vertical-merge-targets
format-columnar-vertical-merge:
	bash ./scripts/format-columnar-vertical-merge.sh

test-columnar-vertical-merge:
	bash ./scripts/test-columnar-vertical-merge.sh

test-race-columnar-vertical-merge:
	bash ./scripts/test-race-columnar-vertical-merge.sh

benchmark-columnar-vertical-merge:
	bash ./scripts/benchmark-columnar-vertical-merge.sh

deliver-columnar-vertical-merge:
	bash ./scripts/deliver-columnar-vertical-merge.sh preview

commit-columnar-vertical-merge:
	bash ./scripts/deliver-columnar-vertical-merge.sh commit

push-columnar-vertical-merge:
	bash ./scripts/deliver-columnar-vertical-merge.sh push
EOF
)
base_makefile="$tmpdir/Makefile"
git show HEAD:Makefile > "$base_makefile"
if ! grep -Fq -- '# columnar-vertical-merge-targets' "$base_makefile"; then
	printf '\n%s\n' "$target_block" >> "$base_makefile"
fi

base_inspiration="$tmpdir/INSPIRATION.md"
git show HEAD:INSPIRATION.md > "$base_inspiration"
child='- [x] C029a Vertical columnar merge loads only requested fields from each part.'
if ! grep -Fq -- "$child" "$base_inspiration"; then
	printf '\n%s\n' "$child" >> "$base_inspiration"
fi

GIT_INDEX_FILE="$index" git read-tree HEAD
makefile_blob=$(git hash-object -w "$base_makefile")
inspiration_blob=$(git hash-object -w "$base_inspiration")
GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$inspiration_blob,INSPIRATION.md"
GIT_INDEX_FILE="$index" git add -- \
	COLUMNAR_VERTICAL_MERGE.md \
	hat/hatSql/columnar_vertical_merge.go \
	hat/hatSql/columnar_vertical_merge_test.go \
	scripts/benchmark-columnar-vertical-merge.sh \
	scripts/deliver-columnar-vertical-merge.sh \
	scripts/format-columnar-vertical-merge.sh \
	scripts/test-columnar-vertical-merge.sh \
	scripts/test-race-columnar-vertical-merge.sh

GIT_INDEX_FILE="$index" git diff --cached --check
if GIT_INDEX_FILE="$index" git diff --cached --quiet; then
	echo "columnar vertical merge already delivered"
	git rev-parse HEAD
	exit 0
fi

echo "columnar vertical merge delivery ($mode)"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == "commit" ]]; then
	GIT_INDEX_FILE="$index" git commit -m "feat(sql): add vertical columnar merge"
	git rev-parse HEAD
fi
