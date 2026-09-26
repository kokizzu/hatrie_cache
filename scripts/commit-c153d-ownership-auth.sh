#!/bin/sh
set -eu

if git diff --cached --quiet; then
    printf '%s\n' 'No staged C153d changes to commit' >&2
    exit 1
fi

git diff --cached --check
git commit -m 'feat(topology): authenticate partition ownership votes'
