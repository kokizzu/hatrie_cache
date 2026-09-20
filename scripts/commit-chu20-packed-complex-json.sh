#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
	printf '%s\n' 'no staged CH-U20 changes'
	exit 1
fi
git commit -m 'perf(sql): pack complex JSON subcolumns'
