#!/bin/sh
set -eu

git diff --cached --check
if git diff --cached --quiet; then
	printf '%s\n' 'no staged M-U01 changes'
	exit 1
fi
git commit -m 'Add durable connector lifecycle snapshots [skip ci]'
