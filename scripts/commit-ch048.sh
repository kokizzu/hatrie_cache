#!/bin/sh
set -eu

if git diff --cached --quiet; then
    printf '%s\n' "no staged CH-048 changes" >&2
    exit 1
fi
git commit -m 'Accelerate plain string SQL predicates [skip ci]'
