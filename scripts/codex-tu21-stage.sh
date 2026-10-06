#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU21_VERSIONED_SPACE_MIGRATION.md \
  Makefile \
  hat/hatSchema/space_migration.go \
  hat/hatSchema/space_migration_test.go \
  scripts/codex-tu21-benchmark.sh \
  scripts/codex-tu21-format.sh \
  scripts/codex-tu21-full-test.sh \
  scripts/codex-tu21-package-test.sh \
  scripts/codex-tu21-race.sh \
  scripts/codex-tu21-test.sh \
  scripts/codex-tu21-vet.sh \
  scripts/codex-tu21-stage.sh \
  scripts/codex-tu21-status.sh \
  scripts/codex-tu21-commit.sh \
  scripts/codex-tu21-push.sh
