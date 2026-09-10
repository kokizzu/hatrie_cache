#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet --; then
  echo "refusing to commit because unrelated changes are already staged" >&2
  exit 1
fi

git add \
  Makefile \
  README.md \
  INSPIRATION.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  BENCHMARK.md \
  COMPILED_TEMPLATE_REUSE.md \
  hat/hatSql/compiled.go \
  hat/hatSql/query.go \
  hat/hatSql/compiled_template_reuse_test.go \
  scripts/format-m071-compiled-template-reuse.sh \
  scripts/test-m071-compiled-template-reuse.sh \
  scripts/benchmark-m071-compiled-template-reuse.sh \
  scripts/test-race-m071-compiled-template-reuse.sh \
  scripts/vet-m071-compiled-template-reuse.sh \
  scripts/review-m071-compiled-template-reuse.sh \
  scripts/commit-m071-compiled-template-reuse.sh \
  scripts/push-m071-compiled-template-reuse.sh

git diff --cached --check
if [[ "${M071_AMEND:-0}" == "1" ]]; then
  git commit --amend --no-edit
else
  git commit -m "optimize static compiled SQL template reuse"
fi
