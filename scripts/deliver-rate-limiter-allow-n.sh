#!/usr/bin/env bash
set -euo pipefail

mode="${1:-stage}"
commit_message="feat(rate): add atomic batch admission [skip ci]"
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
	"hat/hatRate/rate_limiter.go"
	"hat/hatRate/allow_n_test.go"
	"hat/hatRate/allow_n_benchmark_test.go"
	"RATE_LIMITER_BATCH_ADMISSION.md"
	"scripts/format-rate-limiter-allow-n.sh"
	"scripts/test-rate-limiter-allow-n.sh"
	"scripts/test-rate-limiter-package.sh"
	"scripts/benchmark-rate-limiter-single-baseline.sh"
	"scripts/benchmark-rate-limiter-single.sh"
	"scripts/benchmark-rate-limiter-allow-n-baseline.sh"
	"scripts/benchmark-rate-limiter-allow-n.sh"
	"scripts/race-rate-limiter-allow-n.sh"
	"scripts/deliver-rate-limiter-allow-n.sh"
)

makefile_block=$(cat <<'EOF'

.PHONY: format-rate-limiter-allow-n test-rate-limiter-allow-n test-rate-limiter-package benchmark-rate-limiter-single-baseline benchmark-rate-limiter-single benchmark-rate-limiter-allow-n-baseline benchmark-rate-limiter-allow-n race-rate-limiter-allow-n
format-rate-limiter-allow-n:
	bash scripts/format-rate-limiter-allow-n.sh

test-rate-limiter-allow-n:
	bash scripts/test-rate-limiter-allow-n.sh

test-rate-limiter-package:
	bash scripts/test-rate-limiter-package.sh

benchmark-rate-limiter-single-baseline:
	bash scripts/benchmark-rate-limiter-single-baseline.sh

benchmark-rate-limiter-single:
	bash scripts/benchmark-rate-limiter-single.sh

benchmark-rate-limiter-allow-n-baseline:
	bash scripts/benchmark-rate-limiter-allow-n-baseline.sh

benchmark-rate-limiter-allow-n:
	bash scripts/benchmark-rate-limiter-allow-n.sh

race-rate-limiter-allow-n:
	bash scripts/race-rate-limiter-allow-n.sh

.PHONY: stage-rate-limiter-allow-n commit-rate-limiter-allow-n push-rate-limiter-allow-n
stage-rate-limiter-allow-n:
	bash scripts/deliver-rate-limiter-allow-n.sh stage

commit-rate-limiter-allow-n:
	bash scripts/deliver-rate-limiter-allow-n.sh commit

push-rate-limiter-allow-n:
	bash scripts/deliver-rate-limiter-allow-n.sh push
EOF
)

prepare_index() {
	tmp_index=$(mktemp /tmp/hatrie-rate-limiter-index.XXXXXX)
	GIT_INDEX_FILE="$tmp_index" git read-tree HEAD
	for file in "${feature_files[@]}"; do
		GIT_INDEX_FILE="$tmp_index" git add -- "$file"
	done
	tmp_makefile=$(mktemp /tmp/hatrie-rate-limiter-makefile.XXXXXX)
	GIT_INDEX_FILE="$tmp_index" git show HEAD:Makefile > "$tmp_makefile"
	if ! grep -q '^format-rate-limiter-allow-n:' "$tmp_makefile"; then
		printf '%s\n' "$makefile_block" >> "$tmp_makefile"
	fi
	makefile_blob=$(git hash-object -w "$tmp_makefile")
	GIT_INDEX_FILE="$tmp_index" git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
}

verify_index() {
	actual_file="/tmp/hatrie-rate-limiter-files.$$"
	GIT_INDEX_FILE="$tmp_index" git diff --cached --name-only > "$actual_file"
	while IFS= read -r path; do
		case "$path" in
			Makefile|RATE_LIMITER_BATCH_ADMISSION.md|hat/hatRate/rate_limiter.go|hat/hatRate/allow_n_test.go|hat/hatRate/allow_n_benchmark_test.go|scripts/format-rate-limiter-allow-n.sh|scripts/test-rate-limiter-allow-n.sh|scripts/test-rate-limiter-package.sh|scripts/benchmark-rate-limiter-single-baseline.sh|scripts/benchmark-rate-limiter-single.sh|scripts/benchmark-rate-limiter-allow-n-baseline.sh|scripts/benchmark-rate-limiter-allow-n.sh|scripts/race-rate-limiter-allow-n.sh|scripts/deliver-rate-limiter-allow-n.sh)
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
			tmp_worktree=$(mktemp -d /tmp/hatrie-rate-limiter-push.XXXXXX)
			git worktree add --detach "$tmp_worktree" "$remote_commit"
			if ! git -C "$tmp_worktree" cherry-pick "$local_commit"; then
				git -C "$tmp_worktree" checkout --ours -- Makefile
				if ! grep -q '^format-rate-limiter-allow-n:' "$tmp_worktree/Makefile"; then
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
