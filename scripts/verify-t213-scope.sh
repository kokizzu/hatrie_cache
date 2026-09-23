#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git diff --check
git status --short
