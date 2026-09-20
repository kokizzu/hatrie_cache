#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
if git diff --cached --quiet; then
	printf 'nothing staged for T-U33\n' >&2
	exit 1
fi
git commit -m "feat(auth): add function-scoped role grants"
