#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  INSPIRATION_BACKLOG.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  TR028_AUTHENTICATED_KEYSET_TOKENS.md \
  hat/hatSql/query.go \
  hat/hatSql/keyset.go \
  hat/hatSql/sql_keyset_token.go \
  hat/hatSql/tr028_keyset_token_test.go \
  hat/hatSql/tr028_keyset_token_integration_test.go \
  hat/hatSql/tr028_keyset_token_benchmark_test.go \
  scripts/test-tr028-keyset-token.sh \
  scripts/format-tr028-keyset-token.sh \
  scripts/benchmark-tr028-keyset-token.sh \
  scripts/test-tr028-keyset-package.sh \
  scripts/race-tr028-keyset-token.sh \
  scripts/vet-tr028-keyset-token.sh \
  scripts/verify-tr028-docs.sh \
  scripts/status-tr028-keyset-token.sh \
  scripts/stage-tr028-keyset-token.sh \
  scripts/commit-tr028-keyset-token.sh \
  scripts/push-tr028-keyset-token.sh
git diff --cached --check
git diff --cached --stat
