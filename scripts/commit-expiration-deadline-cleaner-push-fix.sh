#!/usr/bin/env bash
set -euo pipefail

git add Makefile scripts/push-expiration-deadline-cleaner.sh scripts/commit-expiration-deadline-cleaner-push-fix.sh
git commit --amend --no-edit
