#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '== status =='
git status --short
printf '%s\n' '== diff check =='
git diff --check
printf '%s\n' '== diff stat =='
git diff --stat
printf '%s\n' '== changed paths =='
git diff --name-only
