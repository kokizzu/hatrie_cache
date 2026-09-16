#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git diff --cached --quiet && { printf '%s\n' 'nothing staged' >&2; exit 1; }
git commit -m '[skip ci] Add adaptive tuple compression'
