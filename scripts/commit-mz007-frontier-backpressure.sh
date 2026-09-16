#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check --
git diff --cached --stat --
git commit -m '[skip ci] Add frontier-aware source backpressure'
