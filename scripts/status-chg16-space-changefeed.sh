#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --check
git diff --cached --check
git diff --stat
git diff --cached --stat
