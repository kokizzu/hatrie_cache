#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "feat: add exactly-once source restart state"
