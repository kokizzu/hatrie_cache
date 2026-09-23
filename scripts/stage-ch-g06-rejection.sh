#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  CH006_RUNTIME_JOIN_PREFETCH.md \
  IDEA_GAP_CATALOG.md \
  Makefile \
  hat/hatSql/contracts.go \
  hat/hatSql/query.go \
  scripts/m225-diff.sh \
  scripts/m269-ch-g06-rejection-docs.sh \
  scripts/stage-ch-g06-rejection.sh \
  scripts/commit-ch-g06-rejection.sh \
  scripts/push-ch-g06-rejection.sh
git diff --cached --check
git status --short
