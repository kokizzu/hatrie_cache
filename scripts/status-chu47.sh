#!/usr/bin/env bash
set -euo pipefail

git status --short
printf '\nChanged paths:\n'
git diff --name-only
git diff --cached --name-only
