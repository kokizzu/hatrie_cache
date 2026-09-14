#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  printf '%s\n' 'no staged TR-044 changes'
  exit 1
fi
git commit -m 'hatPeer: add bounded compact payload compression'
