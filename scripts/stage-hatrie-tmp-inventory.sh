#!/usr/bin/env bash
set -euo pipefail

git add -- Makefile scripts/audit-hatrie-tmp-inventory.sh scripts/stage-hatrie-tmp-inventory.sh scripts/commit-hatrie-tmp-inventory.sh scripts/push-hatrie-tmp-inventory.sh
git diff --cached --check
git diff --cached --stat -- Makefile scripts/audit-hatrie-tmp-inventory.sh scripts/stage-hatrie-tmp-inventory.sh scripts/commit-hatrie-tmp-inventory.sh scripts/push-hatrie-tmp-inventory.sh
