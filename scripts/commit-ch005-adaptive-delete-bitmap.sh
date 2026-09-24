#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
	printf '%s\n' 'no staged changes for CH-005 adaptive delete bitmap' >&2
	exit 1
fi
git commit -m 'perf: adapt persistent delete bitmap encoding'
