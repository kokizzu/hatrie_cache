#!/usr/bin/env bash
set -euo pipefail

plan="$PWD/.hatrie-tmp-cleanup.plan"
if [[ -e "$plan" ]]; then
  rm -f -- "$plan"
  printf 'REMOVED %s\n' "$plan"
else
  printf 'No local Hatrie cleanup plan: %s\n' "$plan"
fi
