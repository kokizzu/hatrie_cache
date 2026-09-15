#!/usr/bin/env bash
set -euo pipefail

mapfile -t staged < <(git diff --cached --name-only)
if ((${#staged[@]} == 0)); then
    printf '%s\n' 'no staged CH-037 changes; commit skipped'
    exit 0
fi
git diff --cached --check
git commit -m 'Add arg-extreme aggregate states'
