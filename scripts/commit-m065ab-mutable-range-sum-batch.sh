#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "perf: batch same-position mutable range sums"
