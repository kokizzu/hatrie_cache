#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "docs: verify incremental top-k coverage"
