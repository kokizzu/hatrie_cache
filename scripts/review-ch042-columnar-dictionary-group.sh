#!/usr/bin/env bash
set -euo pipefail

git diff --check
printf '%s\n' '## changed paths'
git status --short
printf '%s\n' '## diff stat'
git diff --stat -- Makefile ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md MZ026_COLUMNAR_DICTIONARY_GROUP.md \
  hat/hatSql/columnar_vector_group_aggregate.go \
  hat/hatSql/ch042_columnar_dictionary_group.go \
  hat/hatSql/ch042_columnar_dictionary_group_test.go \
  hat/hatSql/ch042_columnar_dictionary_group_benchmark_test.go \
  scripts/test-ch042-columnar-dictionary-group.sh \
  scripts/benchmark-ch042-columnar-dictionary-group.sh \
  scripts/format-ch042-columnar-dictionary-group.sh \
  scripts/test-ch042-columnar-dictionary-group-package.sh \
  scripts/race-ch042-columnar-dictionary-group.sh \
  scripts/vet-ch042-columnar-dictionary-group.sh \
  scripts/review-ch042-columnar-dictionary-group.sh
printf '%s\n' '## recorded benchmark lines'
grep -n -E 'Code-keyed|code-keyed/rows|309878|308846|295653|296419|292714|2679103|2685225|2464920|2450951|2575722' MZ026_COLUMNAR_DICTIONARY_GROUP.md BENCHMARK.md
