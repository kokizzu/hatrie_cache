#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "docs: reconcile C226 hash join spilling"
