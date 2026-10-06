#!/usr/bin/env bash
set -euo pipefail
branch=$(git branch --show-current)
git push -u origin "$branch"
