#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  M040_SOURCE_SCHEMA_REGISTRY.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatSchema/source_schema_registry.go \
  hat/hatSchema/source_schema_registry_test.go \
  scripts/benchmark-m040.sh \
  scripts/commit-m040.sh \
  scripts/format-m040.sh \
  scripts/push-m040.sh \
  scripts/race-m040.sh \
  scripts/test-m040.sh \
  scripts/vet-m040.sh
git diff --cached --check
git commit -m "feat(schema): add bounded source schema registry"
