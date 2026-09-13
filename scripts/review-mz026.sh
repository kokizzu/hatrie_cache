#!/usr/bin/env bash
set -eu

git status --short
git diff --check
git diff --stat
git diff -- hat/hatSql/subscription_snapshot_export.go README.md BENCHMARK.md INSPIRATION_BACKLOG.md Makefile
sed -n '23105,23180p' BENCHMARK.md
