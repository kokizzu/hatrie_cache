#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet --exit-code; then
  echo "no staged CH-012 changes" >&2
  exit 1
fi
git commit -m "feat: add projection advisor cost model [skip ci]"
