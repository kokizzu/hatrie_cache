#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "add arrangement memory breakdown metrics"
