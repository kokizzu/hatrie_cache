#!/usr/bin/env bash
set -euo pipefail

test ! -e scripts/inspect-next.sh
git diff --check
git status --short
git diff --stat
