#!/bin/sh
set -eu

if git diff --cached --quiet; then
	printf '%s\n' 'no staged MZ-045 changes' >&2
	exit 1
fi
git commit -m 'Reuse canonical compiled SQL plans [skip ci]'
