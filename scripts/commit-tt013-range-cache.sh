#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "hatDataStructure: add range tuple cache"
