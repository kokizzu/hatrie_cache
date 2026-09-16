#!/usr/bin/env bash
set -euo pipefail

if [[ -z "$(git diff --cached --name-only)" ]]; then
	printf '%s\n' 'No staged CH-18 changes.' >&2
	exit 1
fi

git diff --cached --check
git commit -m '[skip ci] Add projection refresh status'
