#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'CH-028 package files:'
rg --files hat/hatDictionary
printf '%s\n' 'CH-028 catalog rows:'
rg -n 'CH-28|CH-028|Dictionary version|dictionary version|fallback' --glob '*.md' 2>/dev/null || true
printf '%s\n' 'Working tree:'
git status --short
