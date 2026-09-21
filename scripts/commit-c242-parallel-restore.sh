#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
	printf '%s\n' 'No staged C242 changes to commit.' >&2
	exit 1
fi
git commit -m 'feat(backup): add bounded parallel repository restore'
