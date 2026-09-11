#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --stat
git diff -- \
  hat/hatSql/m052p_auto_native_dataflow.go \
  hat/hatSql/m052v_auto_native_scalar_limit_test.go \
  hat/hatSql/m052v_auto_native_scalar_limit_benchmark_test.go \
  BENCHMARK.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION.md \
  scripts/inspect-native-dataflow.sh \
  Makefile
git status --short
