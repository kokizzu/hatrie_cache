#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  Makefile \
  MZ026_COLUMNAR_DICTIONARY_GROUP.md \
  hat/hatSql/columnar_vector_group_aggregate.go \
  hat/hatSql/ch042_columnar_dictionary_group.go \
  hat/hatSql/ch042_columnar_dictionary_group_benchmark_test.go \
  hat/hatSql/ch042_columnar_dictionary_group_test.go \
  scripts/benchmark-ch042-columnar-dictionary-group.sh \
  scripts/commit-ch042-columnar-dictionary-group.sh \
  scripts/format-ch042-columnar-dictionary-group.sh \
  scripts/push-ch042-columnar-dictionary-group.sh \
  scripts/race-ch042-columnar-dictionary-group.sh \
  scripts/review-ch042-columnar-dictionary-group.sh \
  scripts/stage-ch042-columnar-dictionary-group.sh \
  scripts/test-ch042-columnar-dictionary-group-package.sh \
  scripts/test-ch042-columnar-dictionary-group.sh \
  scripts/vet-ch042-columnar-dictionary-group.sh
git status --short
