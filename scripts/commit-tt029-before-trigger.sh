#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "hatSql: add before trigger lifecycle"
