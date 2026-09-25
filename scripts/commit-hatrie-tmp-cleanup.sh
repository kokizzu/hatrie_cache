#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "fix(test): protect active Hatrie temp worktrees [skip ci]"
