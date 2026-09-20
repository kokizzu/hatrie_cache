#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet --; then
  echo "No staged C247 Gorilla codec changes to commit."
  exit 1
fi

git commit -m "feat(codec): add Gorilla float64 codec"
