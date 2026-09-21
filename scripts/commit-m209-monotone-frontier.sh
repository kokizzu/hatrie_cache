#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "feat: share monotone logical timestamp frontiers"
