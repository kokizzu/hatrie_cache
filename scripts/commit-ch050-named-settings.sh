#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  README.md \
  SQL_NAMED_SETTINGS.md \
  Makefile \
  hat/hatSql/named_settings.go \
  hat/hatSql/named_settings_test.go \
  hat/hatSql/named_settings_benchmark_test.go \
  scripts/benchmark-ch050-named-settings.sh \
  scripts/commit-ch050-named-settings.sh \
  scripts/format-ch050-named-settings.sh \
  scripts/push-ch050-named-settings.sh \
  scripts/review-ch050-named-settings.sh \
  scripts/test-ch050-named-settings.sh \
  scripts/test-race-ch050-named-settings.sh \
  scripts/verify-ch050-named-settings-docs.sh
git commit -m 'sql: add versioned named settings'
