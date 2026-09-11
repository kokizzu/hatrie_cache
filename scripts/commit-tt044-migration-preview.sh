#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md SCHEMA_MIGRATION_DRY_RUN.md hat/hatSchema/schema.go hat/hatSchema/migration_preview_test.go hat/hatSchema/migration_preview_benchmark_test.go scripts/test-tt044-migration-preview.sh scripts/format-tt044-migration-preview.sh scripts/benchmark-tt044-migration-preview.sh scripts/verify-tt044-docs.sh scripts/test-race-tt044-migration-preview.sh scripts/vet-tt044-migration-preview.sh scripts/review-tt044-migration-preview.sh scripts/commit-tt044-migration-preview.sh scripts/push-tt044-migration-preview.sh
git diff --cached --check
git commit -m 'hatSchema: add migration preview'
