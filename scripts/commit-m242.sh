#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "adopt per-operator SQL metrics"
