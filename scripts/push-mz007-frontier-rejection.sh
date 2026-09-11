#!/usr/bin/env bash
set -euo pipefail

branch="$(git branch --show-current)"
if [[ -n "$branch" ]]; then
	git push origin "$branch"
	exit 0
fi

remote_head="$(git symbolic-ref --short refs/remotes/origin/HEAD)"
target="${remote_head#origin/}"
if [[ -z "$target" || "$target" == "$remote_head" ]]; then
	printf 'cannot determine the default remote branch from %s\n' "$remote_head" >&2
	exit 1
fi
git push origin "HEAD:$target"
