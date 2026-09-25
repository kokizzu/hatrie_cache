#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --cached --check
git diff --cached --stat
git commit -m 'feat: add rotating subscription wire keyring [skip ci]'
