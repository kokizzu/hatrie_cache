#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

if git diff --cached --quiet; then
	printf '%s\n' 'refusing to commit T047g: the index is empty' >&2
	exit 1
fi
git diff --cached --check
git commit -m 'feat(replication): persist participant commit state'
