#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --stat
git diff -- BENCHMARK.md INSPIRATION_ROUND2.md Makefile
git status --short
