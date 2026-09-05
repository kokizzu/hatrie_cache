#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --cached --check
git diff --cached --stat
git status --short
