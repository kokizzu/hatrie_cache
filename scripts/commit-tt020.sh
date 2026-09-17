#!/bin/sh
set -eu

if git diff --cached --quiet; then
	printf '%s\n' 'no staged TT-020 changes' >&2
	exit 1
fi
git commit -m 'Add generic ordered index ranges [skip ci]'
