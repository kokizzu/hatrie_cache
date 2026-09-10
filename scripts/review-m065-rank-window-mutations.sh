#!/usr/bin/env bash
set -eu

git diff --check
printf '%s\n' '--- changed tracked paths ---'
git diff --name-only
printf '%s\n' '--- changed untracked paths ---'
git ls-files --others --exclude-standard
printf '%s\n' '--- diff stat ---'
git diff --stat
