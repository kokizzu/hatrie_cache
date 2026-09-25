#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --cached --check
git diff --cached --stat
git commit -m 'chore: audit and clean stale Hatrie temp builds [skip ci]'
