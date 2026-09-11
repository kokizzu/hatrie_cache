#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  README.md \
  SQL_EXTERNAL_DICTIONARIES.md \
  Makefile \
  hat/hatSql/ch049_external_dictionary_baseline_benchmark_test.go \
  hat/hatSql/external_dictionary.go \
  hat/hatSql/external_dictionary_test.go \
  hat/hatSql/external_dictionary_benchmark_test.go \
  scripts/benchmark-ch049-external-dictionary.sh \
  scripts/commit-ch049-external-dictionary.sh \
  scripts/format-ch049-external-dictionary.sh \
  scripts/push-ch049-external-dictionary.sh \
  scripts/review-ch049-external-dictionary.sh \
  scripts/test-ch049-external-dictionary.sh \
  scripts/test-race-ch049-external-dictionary.sh \
  scripts/verify-ch049-external-dictionary-docs.sh
git commit -m 'sql: add refreshable external dictionaries'
