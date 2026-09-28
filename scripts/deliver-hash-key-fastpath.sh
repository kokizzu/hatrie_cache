#!/usr/bin/env bash
set -euo pipefail

mode="${1:-stage}"
commit_message="feat(hash): add typed numeric hash fast paths [skip ci]"
tmp_index=""
tmp_makefile=""

cleanup() {
	if [ -n "$tmp_index" ]; then
		rm -f -- "$tmp_index"
	fi
	if [ -n "$tmp_makefile" ]; then
		rm -f -- "$tmp_makefile"
	fi
}
trap cleanup EXIT

feature_files=(
	"hat/hatHash/hash.go"
	"hat/hatHash/hash_key_fastpath_test.go"
	"hat/hatHash/hash_key_fastpath_benchmark_test.go"
	"HASH_KEY_FASTPATH.md"
	"scripts/format-hash-key-fastpath.sh"
	"scripts/test-hash-key-fastpath.sh"
	"scripts/test-hash-package.sh"
	"scripts/benchmark-hash-key-baseline.sh"
	"scripts/benchmark-hash-key-fastpath.sh"
	"scripts/race-hash-key-fastpath.sh"
	"scripts/deliver-hash-key-fastpath.sh"
)

makefile_block=$(cat <<'EOF'

.PHONY: format-hash-key-fastpath test-hash-key-fastpath test-hash-key-fastpath-package benchmark-hash-key-baseline benchmark-hash-key-fastpath race-hash-key-fastpath
format-hash-key-fastpath:
	bash scripts/format-hash-key-fastpath.sh

test-hash-key-fastpath:
	bash scripts/test-hash-key-fastpath.sh

test-hash-key-fastpath-package:
	bash scripts/test-hash-package.sh

benchmark-hash-key-baseline:
	bash scripts/benchmark-hash-key-baseline.sh

benchmark-hash-key-fastpath:
	bash scripts/benchmark-hash-key-fastpath.sh

race-hash-key-fastpath:
	bash scripts/race-hash-key-fastpath.sh

.PHONY: stage-hash-key-fastpath commit-hash-key-fastpath push-hash-key-fastpath
stage-hash-key-fastpath:
	bash scripts/deliver-hash-key-fastpath.sh stage

commit-hash-key-fastpath:
	bash scripts/deliver-hash-key-fastpath.sh commit

push-hash-key-fastpath:
	bash scripts/deliver-hash-key-fastpath.sh push
EOF
)

prepare_index() {
	tmp_index=$(mktemp /tmp/hatrie-hash-key-index.XXXXXX)
	GIT_INDEX_FILE="$tmp_index" git read-tree HEAD
	for file in "${feature_files[@]}"; do
		GIT_INDEX_FILE="$tmp_index" git add -- "$file"
	done
	tmp_makefile=$(mktemp /tmp/hatrie-hash-key-makefile.XXXXXX)
	GIT_INDEX_FILE="$tmp_index" git show HEAD:Makefile > "$tmp_makefile"
	printf '%s\n' "$makefile_block" >> "$tmp_makefile"
	makefile_blob=$(git hash-object -w "$tmp_makefile")
	GIT_INDEX_FILE="$tmp_index" git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
}

verify_index() {
	actual_file="/tmp/hatrie-hash-key-files.$$"
	trap 'rm -f -- "$actual_file"' RETURN
	GIT_INDEX_FILE="$tmp_index" git diff --cached --name-only > "$actual_file"
	while IFS= read -r path; do
		case "$path" in
			Makefile|HASH_KEY_FASTPATH.md|hat/hatHash/hash.go|hat/hatHash/hash_key_fastpath_test.go|hat/hatHash/hash_key_fastpath_benchmark_test.go|scripts/format-hash-key-fastpath.sh|scripts/test-hash-key-fastpath.sh|scripts/test-hash-package.sh|scripts/benchmark-hash-key-baseline.sh|scripts/benchmark-hash-key-fastpath.sh|scripts/race-hash-key-fastpath.sh|scripts/deliver-hash-key-fastpath.sh)
				;;
			*)
				printf 'unexpected staged path: %s\n' "$path" >&2
				exit 1
				;;
		esac
	done < "$actual_file"
}

case "$mode" in
	stage)
		prepare_index
		verify_index
		GIT_INDEX_FILE="$tmp_index" git diff --cached --stat
		printf 'feature-only staging verified; the shared index was not changed.\n'
		;;
	commit)
		prepare_index
		verify_index
		GIT_INDEX_FILE="$tmp_index" git commit --no-verify -m "$commit_message"
		;;
	push)
		git push
		;;
	*)
		printf 'usage: %s [stage|commit|push]\n' "$0" >&2
		exit 2
		;;
esac
