#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "perf: skip zero-estimate compaction bookkeeping [skip ci]"
