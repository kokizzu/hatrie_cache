#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --cached --check
test ! -e scripts/inspect-ch046.sh
