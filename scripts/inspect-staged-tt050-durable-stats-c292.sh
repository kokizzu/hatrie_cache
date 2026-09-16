#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
	echo "no staged TT-050 changes" >&2
	exit 1
fi

git diff --cached --check
printf '%s\n' '--- staged TT-050 paths ---'
git diff --cached --name-status
printf '%s\n' '--- staged TT-050 diff stat ---'
git diff --cached --stat
