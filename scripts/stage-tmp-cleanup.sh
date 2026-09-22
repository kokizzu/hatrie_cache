#!/usr/bin/env bash
set -euo pipefail

git add -- Makefile scripts/cleanup-hatrie-tmp-audit.sh scripts/stage-tmp-cleanup.sh scripts/commit-tmp-cleanup.sh scripts/push-tmp-cleanup.sh
git diff --cached --check
git diff --cached --name-status
