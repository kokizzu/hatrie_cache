#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "hatFiber: add tenant scheduler quotas"
