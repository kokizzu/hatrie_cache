#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "hat: persist spillable arrangement reopen index"
