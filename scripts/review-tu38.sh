#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short --branch
git diff --stat
rg -n 'direct winner|Existing direct|T-U38 explicit|47\.6|296\.0|6\.223' TR038_CONFLICT_INTROSPECTION.md BENCHMARK.md || true
