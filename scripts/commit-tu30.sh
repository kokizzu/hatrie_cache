#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
if git diff --cached --quiet; then
	printf '%s\n' 'no staged T-U30 changes' >&2
	exit 1
fi
git commit -m 'feat(fiber): add bounded cooperative scheduler'
