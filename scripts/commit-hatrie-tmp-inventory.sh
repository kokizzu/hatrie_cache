#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet -- Makefile scripts/audit-hatrie-tmp-inventory.sh scripts/stage-hatrie-tmp-inventory.sh scripts/commit-hatrie-tmp-inventory.sh scripts/push-hatrie-tmp-inventory.sh; then
    printf '%s\n' "No staged Hatrie temporary inventory changes."
    exit 1
fi

git commit -m "chore: audit temporary Hatrie artifacts"
