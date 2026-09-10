#!/usr/bin/env bash
set -eu

printf '%s\n' '--- current status ---'
git status --short
printf '%s\n' '--- recent feature commits ---'
git log -12 --oneline
printf '%s\n' '--- open inspiration items ---'
rg -n -A1 -B1 '^[-*] \[ \]' INSPIRATION.md
