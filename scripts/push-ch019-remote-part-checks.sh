#!/usr/bin/env bash
set -euo pipefail

branch=$(git branch --show-current)
git push origin "$branch"
