#!/usr/bin/env bash
set -euo pipefail

branch="$(git branch --show-current)"
if [[ -n "$branch" ]]; then
	git push origin "$branch"
	exit 0
fi

remote_head="$(git symbolic-ref --short refs/remotes/origin/HEAD 2>/dev/null || true)"
target="${remote_head#origin/}"
if [[ -z "$target" || "$target" == "$remote_head" ]]; then
	echo "cannot determine the remote default branch from detached HEAD" >&2
	exit 1
fi
git push origin "HEAD:$target"
