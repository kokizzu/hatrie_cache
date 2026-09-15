#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m 'Optimize small R-tree search results'
