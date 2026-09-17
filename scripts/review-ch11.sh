#!/bin/sh
set -eu

printf '%s\n' '--- status ---'
git status --short
printf '%s\n' '--- changed paths ---'
git diff --name-only
printf '%s\n' '--- diff stat ---'
git diff --stat
printf '%s\n' '--- diff check ---'
git diff --check
