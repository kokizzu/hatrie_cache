#!/usr/bin/env bash
set -euo pipefail

branch="$(git branch --show-current)"
if [[ -n "$branch" ]]; then
	git push origin "HEAD:refs/heads/$branch"
	exit 0
fi

target_branch="${PUSH_BRANCH:-master}"
if ! git check-ref-format --branch "$target_branch" >/dev/null; then
	printf 'invalid PUSH_BRANCH: %s\n' "$target_branch" >&2
	exit 1
fi

git push origin "HEAD:refs/heads/$target_branch"
