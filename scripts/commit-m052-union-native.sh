#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --cached --check
git diff --cached --stat
git commit -m 'feat: compose native SQL fragments across unions [skip ci]'
