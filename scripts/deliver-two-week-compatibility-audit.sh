#!/bin/sh
set -eu

paths='Makefile scripts/audit-two-week-compatibility.sh scripts/deliver-two-week-compatibility-audit.sh'

if ! git diff --cached --quiet; then
	echo 'refusing to deliver with pre-existing staged changes' >&2
	exit 1
fi

git diff --check -- $paths
git add -- $paths
git diff --cached --check
git commit -m 'chore: add two-week compatibility audit'
git push
