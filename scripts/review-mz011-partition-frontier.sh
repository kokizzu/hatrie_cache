#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --cached --check
git status --short
