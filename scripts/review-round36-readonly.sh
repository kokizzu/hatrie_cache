#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git diff --stat
git diff --cached --check
git diff --cached --stat
