#!/usr/bin/env bash
set -euo pipefail

git diff --cached --quiet && {
	printf '%s\n' 'no staged CH-021 changes to commit' >&2
	exit 1
}
git commit -m 'feat: add opt-in object storage tiered reads'
