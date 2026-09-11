#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --stat
git diff -- hat/hatSql/m052p_auto_native_dataflow.go BENCHMARK.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION.md Makefile
git status --short
