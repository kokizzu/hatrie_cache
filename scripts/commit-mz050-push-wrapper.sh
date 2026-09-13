#!/usr/bin/env bash
set -euo pipefail

git add Makefile scripts/push-mz050.sh scripts/commit-mz050-push-wrapper.sh
git commit -m "chore: fix MZ-050 push wrapper"
