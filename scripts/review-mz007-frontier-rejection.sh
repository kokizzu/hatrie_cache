#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --stat
git diff --check
git diff -- SQL_SOURCE_FRONTIERS.md
