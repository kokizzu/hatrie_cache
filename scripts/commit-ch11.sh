#!/bin/sh
set -eu

if git diff --cached --quiet; then
	printf '%s\n' 'no staged CH-11 changes' >&2
	exit 1
fi
git commit -m 'Add request-level insert quorums [skip ci]'
