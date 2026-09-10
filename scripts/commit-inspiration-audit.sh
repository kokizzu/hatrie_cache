#!/usr/bin/env bash
set -euo pipefail

git add \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  INSPIRATION.md \
  README.md \
  Makefile \
  scripts/generate-inspiration-audit.sh \
  scripts/verify-inspiration-audit.sh \
  scripts/commit-inspiration-audit.sh
git diff --cached --check
git commit -m 'docs: add product inspiration audit'
git push origin master
