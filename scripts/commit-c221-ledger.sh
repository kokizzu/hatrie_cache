#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "docs: reconcile C221 with ties coverage"
