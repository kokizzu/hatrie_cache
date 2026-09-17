#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "feat: add frontier-safe projection backfill handoff [skip ci]"
