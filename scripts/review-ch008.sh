#!/usr/bin/env bash
set -euo pipefail

git diff --check
printf '%s\n' '--- changed paths ---'
git status --short
printf '%s\n' '--- diff stat ---'
git diff --stat
printf '%s\n' '--- temporary diagnostic paths ---'
if git status --short | rg 'inspect-ch008|/tmp/'; then
	printf '%s\n' 'temporary diagnostic path found'
	exit 1
fi
printf '%s\n' 'none'
