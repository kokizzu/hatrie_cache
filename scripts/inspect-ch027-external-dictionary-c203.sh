#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'CH-027 package files:'
rg --files hat/hatDictionary
printf '%s\n' 'CH-027 catalog rows:'
rg -n 'CH-027|CH-27|External dictionary cache|external dictionary' --glob '*.md' 2>/dev/null || true
printf '%s\n' 'Working tree:'
git status --short
