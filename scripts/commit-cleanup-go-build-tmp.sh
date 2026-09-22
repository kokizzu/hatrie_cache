#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "expose safe Go build temp cleanup"
