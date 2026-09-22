#!/usr/bin/env bash
set -euo pipefail

git diff --cached --quiet && {
	printf '%s\n' 'no staged T202 changes; run make stage-t202 first' >&2
	exit 1
}
git commit -m 'feat: add replica-set leader election'
