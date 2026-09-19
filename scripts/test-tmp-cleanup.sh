#!/usr/bin/env bash
set -euo pipefail

for script in \
  scripts/audit-hatrie-tmp.sh \
  scripts/audit-go-temp-contents.sh \
  scripts/cleanup-go-test-tmp.sh \
  scripts/commit-tmp-cleanup.sh; do
  bash -n "$script"
done

printf 'temporary cleanup scripts: syntax ok\n'
