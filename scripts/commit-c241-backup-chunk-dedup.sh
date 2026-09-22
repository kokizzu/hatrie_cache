#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "feat: deduplicate incremental backup chunks"
