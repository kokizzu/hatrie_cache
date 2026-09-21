#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
	echo "no staged CH-042 changes to commit" >&2
	exit 1
fi
git commit -m "hatSql: add storage-aware sampling pushdown"
