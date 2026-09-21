#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
if git diff --cached --quiet; then
	echo 'no staged CH-041 changes to commit' >&2
	exit 1
fi
git commit -m 'hatSql: add multi-argument GROUPING_ID'
