#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
	echo "no staged CH048 NULL changes to commit" >&2
	exit 1
fi

git commit -m "feat(sql): accelerate null predicates"
