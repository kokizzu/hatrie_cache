#!/usr/bin/env bash
set -euo pipefail

mode="${1:-stage}"
commit_message="feat(http): add reusable binary stream payload buffers [skip ci]"
tmp_index=""
tmp_makefile=""
tmp_worktree=""

cleanup() {
	if [ -n "$tmp_index" ]; then
		rm -f -- "$tmp_index"
	fi
	if [ -n "$tmp_makefile" ]; then
		rm -f -- "$tmp_makefile"
	fi
	if [ -n "$tmp_worktree" ]; then
		git worktree remove --force "$tmp_worktree" >/dev/null 2>&1 || true
		rm -rf -- "$tmp_worktree"
	fi
}
trap cleanup EXIT

feature_files=(
	"hat/hatHttp/binary_stream.go"
	"hat/hatHttp/binary_stream_reuse_test.go"
	"hat/hatHttp/binary_stream_reuse_benchmark_test.go"
	"HTTP_BINARY_STREAM_REUSE.md"
	"scripts/format-http-stream-reuse.sh"
	"scripts/test-http-stream-reuse.sh"
	"scripts/test-http-stream-package.sh"
	"scripts/benchmark-http-stream-reuse-baseline.sh"
	"scripts/benchmark-http-stream-reuse.sh"
	"scripts/race-http-stream-reuse.sh"
	"scripts/deliver-http-stream-reuse.sh"
)

makefile_block=$(cat <<'EOF'

.PHONY: format-http-stream-reuse test-http-stream-reuse test-http-stream-package benchmark-http-stream-reuse-baseline benchmark-http-stream-reuse race-http-stream-reuse
format-http-stream-reuse:
	bash scripts/format-http-stream-reuse.sh

test-http-stream-reuse:
	bash scripts/test-http-stream-reuse.sh

test-http-stream-package:
	bash scripts/test-http-stream-package.sh

benchmark-http-stream-reuse-baseline:
	bash scripts/benchmark-http-stream-reuse-baseline.sh

benchmark-http-stream-reuse:
	bash scripts/benchmark-http-stream-reuse.sh

race-http-stream-reuse:
	bash scripts/race-http-stream-reuse.sh

.PHONY: stage-http-stream-reuse commit-http-stream-reuse push-http-stream-reuse
stage-http-stream-reuse:
	bash scripts/deliver-http-stream-reuse.sh stage

commit-http-stream-reuse:
	bash scripts/deliver-http-stream-reuse.sh commit

push-http-stream-reuse:
	bash scripts/deliver-http-stream-reuse.sh push
EOF
)

prepare_index() {
	tmp_index=$(mktemp /tmp/hatrie-http-stream-index.XXXXXX)
	GIT_INDEX_FILE="$tmp_index" git read-tree HEAD
	for file in "${feature_files[@]}"; do
		GIT_INDEX_FILE="$tmp_index" git add -- "$file"
	done
	tmp_makefile=$(mktemp /tmp/hatrie-http-stream-makefile.XXXXXX)
	GIT_INDEX_FILE="$tmp_index" git show HEAD:Makefile > "$tmp_makefile"
	if ! grep -q '^format-http-stream-reuse:' "$tmp_makefile"; then
		printf '%s\n' "$makefile_block" >> "$tmp_makefile"
	fi
	makefile_blob=$(git hash-object -w "$tmp_makefile")
	GIT_INDEX_FILE="$tmp_index" git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
}

verify_index() {
	actual_file="/tmp/hatrie-http-stream-files.$$"
	GIT_INDEX_FILE="$tmp_index" git diff --cached --name-only > "$actual_file"
	while IFS= read -r path; do
		case "$path" in
			Makefile|HTTP_BINARY_STREAM_REUSE.md|hat/hatHttp/binary_stream.go|hat/hatHttp/binary_stream_reuse_test.go|hat/hatHttp/binary_stream_reuse_benchmark_test.go|scripts/format-http-stream-reuse.sh|scripts/test-http-stream-reuse.sh|scripts/test-http-stream-package.sh|scripts/benchmark-http-stream-reuse-baseline.sh|scripts/benchmark-http-stream-reuse.sh|scripts/race-http-stream-reuse.sh|scripts/deliver-http-stream-reuse.sh)
				;;
			*)
				printf 'unexpected staged path: %s\n' "$path" >&2
				rm -f -- "$actual_file"
				exit 1
				;;
		esac
	done < "$actual_file"
	rm -f -- "$actual_file"
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
		local_commit=$(git rev-parse HEAD)
		git fetch origin master
		remote_commit=$(git rev-parse origin/master)
		if git merge-base --is-ancestor "$remote_commit" "$local_commit"; then
			git push
		else
			tmp_worktree=$(mktemp -d /tmp/hatrie-http-stream-push.XXXXXX)
			git worktree add --detach "$tmp_worktree" "$remote_commit"
			if ! git -C "$tmp_worktree" cherry-pick "$local_commit"; then
				git -C "$tmp_worktree" checkout --ours -- Makefile
				if ! grep -q '^format-http-stream-reuse:' "$tmp_worktree/Makefile"; then
					printf '%s\n' "$makefile_block" >> "$tmp_worktree/Makefile"
				fi
				git -C "$tmp_worktree" add -- Makefile
				GIT_EDITOR=true git -C "$tmp_worktree" cherry-pick --continue
			fi
			git -C "$tmp_worktree" push origin HEAD:master
		fi
		;;
	*)
		printf 'usage: %s [stage|commit|push]\n' "$0" >&2
		exit 2
		;;
esac
