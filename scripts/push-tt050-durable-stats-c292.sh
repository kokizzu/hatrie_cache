#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
	echo "refusing push with staged changes" >&2
	exit 1
fi
git push origin HEAD:master
