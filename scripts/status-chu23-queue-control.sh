#!/usr/bin/env bash
set -euo pipefail

git status --short --branch
git diff --check
git diff --cached --check
git diff --cached --stat
