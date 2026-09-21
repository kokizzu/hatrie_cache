#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
if git diff --cached --quiet; then
	echo 'no staged MZ-009 changes to commit' >&2
	exit 1
fi
git commit -m 'hatSql: push down literal VALID_AT lower bounds'
