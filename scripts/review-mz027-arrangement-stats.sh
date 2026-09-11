#!/usr/bin/env bash
set -euo pipefail

test ! -e scripts/inspect-next-ideas.sh
git diff --check
git status --short
git diff --stat
