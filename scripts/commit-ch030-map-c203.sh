#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
	printf '%s\n' 'no staged CH030 changes to commit' >&2
	exit 1
fi
git commit -m 'add ClickHouse-style map subcolumn pruning'
