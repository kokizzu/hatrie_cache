#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  README.md \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  SQL_TEXT_PHRASE.md \
  hat/hatCache/sql_query.go \
  hat/hatCache/sql_text_phrase.go \
  hat/hatCache/sql_text_phrase_benchmark_test.go \
  hat/hatCache/sql_text_phrase_test.go \
  hat/hatSql/contracts.go \
  hat/hatSql/query.go \
  hat/hatSql/rewrite.go \
  hat/hatSql/text_phrase.go \
  hat/hatSql/text_phrase_benchmark_test.go \
  hat/hatSql/text_phrase_test.go \
  hat/hatSql/text_proximity.go \
  scripts/benchmark-ch026-after.sh \
  scripts/benchmark-ch026-before.sh \
  scripts/check-ch026.sh \
  scripts/commit-ch026.sh \
  scripts/format-ch026.sh \
  scripts/push-ch026.sh \
  scripts/race-ch026.sh \
  scripts/test-ch026-all.sh \
  scripts/test-ch026-text-phrase-red.sh \
  scripts/test-ch026.sh \
  scripts/vet-ch026.sh
git diff --cached --check
git commit -m 'feat: add SQL phrase and proximity search'
