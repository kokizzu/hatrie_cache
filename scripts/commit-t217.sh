#!/usr/bin/env bash
set -euo pipefail

make verify-t217-scope
if git diff --cached --quiet; then
	printf 'No staged T217 changes; run make stage-t217 first.\n' >&2
	exit 1
fi
git commit -m 'feat: add columnar batch ingest'
