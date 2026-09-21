#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
if git diff --cached --quiet; then
	printf '%s\n' 'No staged C243 changes to commit.' >&2
	exit 1
fi
git commit -m "docs(storage): record C243 remote-part cache"
