#!/usr/bin/env bash
set -euo pipefail

git diff --cached --quiet && {
	printf '%s\n' 'commit-aggregate-envelope-c223: no staged changes' >&2
	exit 1
}
git commit -m 'Add versioned aggregate state envelopes'
