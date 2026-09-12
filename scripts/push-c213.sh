#!/usr/bin/env bash
set -euo pipefail

expected_remote="${EXPECTED_REMOTE:-603202227c071cb825227ef23ac08afa0c1357b7}"
remote_head="$(git ls-remote origin refs/heads/master | cut -f1)"
if [[ "$remote_head" != "$expected_remote" ]]; then
	printf 'refusing push: expected origin/master %s, got %s\n' "$expected_remote" "$remote_head" >&2
	exit 1
fi

git push origin HEAD:master
