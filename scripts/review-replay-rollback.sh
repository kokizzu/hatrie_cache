#!/bin/sh
set -eu

git diff --check
git status --short
git diff --stat -- BENCHMARK.md INSPIRATION.md Makefile
