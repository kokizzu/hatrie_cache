#!/usr/bin/env bash
set -eu

git status --short
git diff --stat
git diff --check
sed -n '1,32p' CH009_ASYNC_INSERT_BUFFER.md
sed -n '1,28p' BENCHMARK.md
rg -n '^## CH-009|CH-009|CH009_ASYNC' BENCHMARK.md README.md INSPIRATION_BACKLOG.md
