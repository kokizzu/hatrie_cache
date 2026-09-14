#!/usr/bin/env bash
set -euo pipefail

printf 'Staged paths:\n'
git diff --cached --name-only
printf 'Staged diffstat:\n'
git diff --cached --stat
printf 'Staged checks:\n'
git diff --cached --check
