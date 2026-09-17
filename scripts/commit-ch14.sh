#!/bin/sh
set -eu

if git diff --cached --quiet; then
	printf '%s\n' 'no staged CH-14 changes' >&2
	exit 1
fi
git commit -m 'Add resumable mutation dependency graph [skip ci]'
