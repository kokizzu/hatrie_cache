#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
	echo "no staged CH-026 changes to commit" >&2
	exit 1
fi
git commit -m "hatStorage: add compaction selector policies"
