#!/bin/sh
set -eu

git diff --check
git diff --stat
test -s SQL_ENUM_TYPES.md
rg -n '^## CH-035:|SQL_ENUM_TYPES.md|\| CH-35 \|.*\[x\]' BENCHMARK.md README.md INSPIRATION_BACKLOG.md
