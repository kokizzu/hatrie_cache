#!/usr/bin/env bash
set -eu
git add Makefile README.md INSPIRATION_BACKLOG.md BENCHMARK.md TR046_SCHEMA_DDL_DISCOVERY.md \
  hat/hatSchema/tr046_schema_discovery.go hat/hatSchema/tr046_schema_discovery_test.go \
  scripts/format-tr046-schema-discovery.sh scripts/test-tr046-schema-discovery.sh \
  scripts/test-tr046-package.sh scripts/benchmark-tr046-baseline.sh \
  scripts/benchmark-tr046-schema-discovery.sh scripts/race-tr046-schema-discovery.sh \
  scripts/vet-tr046-schema-discovery.sh scripts/verify-tr046-schema-discovery-docs.sh \
  scripts/review-tr046-schema-discovery.sh scripts/stage-tr046-schema-discovery.sh \
  scripts/commit-tr046-schema-discovery.sh scripts/push-tr046-schema-discovery.sh
