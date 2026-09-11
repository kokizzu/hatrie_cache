#!/usr/bin/env bash
set -euo pipefail

test ! -e scripts/inspect-files.sh
git diff --check
git status --short
git diff --stat
