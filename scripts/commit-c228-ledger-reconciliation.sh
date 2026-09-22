#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "docs: reconcile C228 external sort spilling"
