#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
    printf 'no staged CH-036 changes; commit already exists\n'
    exit 0
fi
git diff --cached --check
git commit -m "Add SQL aggregate state combinators"
