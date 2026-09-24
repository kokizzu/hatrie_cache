#!/usr/bin/env bash
set -euo pipefail

git add scripts/commit-mu035.sh scripts/push-mu035.sh
git diff --cached --check
git commit -m "build: keep M-U35 helper targets usable"
