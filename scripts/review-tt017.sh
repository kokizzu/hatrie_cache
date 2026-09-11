#!/usr/bin/env bash
set -euo pipefail

test ! -e scripts/inspect-tt017.sh
git diff --check
git status --short
git diff --stat
