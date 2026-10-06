#!/usr/bin/env bash
set -euo pipefail

git status --short
printf '%s\n' '== diff stat =='
git diff --stat
printf '%s\n' '== diff check =='
git diff --check
