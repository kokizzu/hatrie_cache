#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m 'feat(sql): accelerate columnar array joins'
