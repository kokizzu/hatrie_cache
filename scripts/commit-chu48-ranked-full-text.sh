#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
if git diff --cached --quiet; then
	printf '%s\n' 'No staged CHU48 changes.' >&2
	exit 1
fi
git commit -m 'feat: add ranked full-text postings and safe tmp cleanup [skip ci]'
