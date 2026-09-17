#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "feat: add transactional SQL view catalog updates [skip ci]"
