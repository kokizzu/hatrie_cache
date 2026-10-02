#!/usr/bin/env bash
set -euo pipefail

git add -- Makefile scripts/deliver-tu09-cleanup.sh
git diff --cached --check
git commit -m 'chore: remove temporary T-U09 delivery target [skip ci]'
git push
