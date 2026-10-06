#!/usr/bin/env bash
set -euo pipefail
git diff --cached --check
git diff --cached --quiet && {
	printf '%s\n' 'no staged C240 changes to commit' >&2
	exit 1
}
git commit -m 'feat: add verified read-only backup streams [skip ci]'
