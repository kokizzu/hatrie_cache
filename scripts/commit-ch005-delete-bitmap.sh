#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "feat: bind delete bitmaps to part manifests"
