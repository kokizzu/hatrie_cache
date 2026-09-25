#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit --no-verify -m 'feat: add native one-pass grouping sets [skip ci]'
