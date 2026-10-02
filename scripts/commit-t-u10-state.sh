#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
if git diff --cached --quiet; then
	printf 'no staged T-U10 changes\n' >&2
	exit 1
fi
git commit -m 'feat(replication): add journal write quorum state [skip ci]'
