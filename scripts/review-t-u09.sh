#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --cached --check
test -f hat/hatReplication/join_bootstrap.go
test -f TU09_JOIN_BOOTSTRAP.md
rg -n 'T-U09|JoinBootstrap' \
  README.md BENCHMARK.md PRODUCT_IDEA_GAPS.md TU09_JOIN_BOOTSTRAP.md \
  hat/hatReplication/join_bootstrap.go
git status --short
