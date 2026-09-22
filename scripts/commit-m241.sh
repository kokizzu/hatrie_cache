#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "M241 add optimizer rule trace"
