#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git diff --cached --quiet && {
	printf '%s\n' 'no staged TT-048 changes' >&2
	exit 1
}
git commit -m '[skip ci] Add durable priority visibility queue'
