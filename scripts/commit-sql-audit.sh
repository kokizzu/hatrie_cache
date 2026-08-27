#!/usr/bin/env sh
set -eu

git add -- \
    Makefile \
    scripts/audit-sql-capabilities.sh \
    scripts/audit-hatcache-boundaries.sh \
    scripts/verify-sql-capabilities.sh \
    scripts/commit-sql-audit.sh
git commit -m 'chore: add SQL capability audit targets'
git push
