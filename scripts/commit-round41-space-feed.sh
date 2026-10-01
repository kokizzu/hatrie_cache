#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git diff --cached --quiet && { printf '%s\n' 'nothing staged for T-U39 space feed' >&2; exit 1; }
git commit -m 'hatCache: add versioned space changefeed [skip ci]'
