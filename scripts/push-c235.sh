#!/usr/bin/env bash
set -euo pipefail

git fetch origin master
remote_head="$(git rev-parse origin/master)"
local_head="$(git rev-parse HEAD)"
if ! git merge-base --is-ancestor "$remote_head" "$local_head"; then
	printf 'origin/master moved; refusing to overwrite %s\n' "$remote_head" >&2
	exit 1
fi
git push origin HEAD:master
