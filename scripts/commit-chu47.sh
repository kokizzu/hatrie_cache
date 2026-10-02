#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git diff --cached --name-status
git commit -m "feat(sql): add atomic dictionary functions [skip ci]"
