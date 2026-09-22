#!/usr/bin/env bash
set -euo pipefail

git diff --cached --quiet && {
	printf '%s\n' 'no staged T203 changes; run make stage-t203 first' >&2
	exit 1
}
git commit -m 'feat: add strict leader write fencing'
