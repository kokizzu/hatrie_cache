#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
    printf '%s\n' 'no staged CH-020 changes to commit' >&2
    exit 1
fi

git commit -m 'feat: add zero-copy remote part registry'
