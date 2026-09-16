#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
	echo "no staged TT-050 changes to commit" >&2
	exit 1
fi
git diff --cached --check
git commit -m '[skip ci] Persist source-versioned SQL planner statistics'
