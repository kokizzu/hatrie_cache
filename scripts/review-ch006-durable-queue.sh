#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --check
git diff --stat
git diff --cached --check
git diff --cached --stat
git diff --cached --name-status
