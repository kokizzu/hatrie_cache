#!/usr/bin/env bash
set -euo pipefail

git add -- \
    ADOPTED_QUERY_ENGINE_IDEAS.md \
    BENCHMARK.md \
    CH004_FINAL_READ.md \
    Makefile \
    hat/hatSql/ch004_final.go \
    hat/hatSql/ch004_schema.go \
    hat/hatSql/ch004_schema_benchmark_test.go \
    hat/hatSql/ch004_schema_test.go \
    hat/hatSql/query.go \
    scripts/benchmark-ch004-schema-before.sh \
    scripts/benchmark-ch004-schema-compare.sh \
    scripts/benchmark-ch004-schema.sh \
    scripts/commit-ch004-schema.sh \
    scripts/format-ch004-schema.sh \
    scripts/push-ch004-schema.sh \
    scripts/prepare-ch004-schema-branch.sh \
    scripts/race-ch004-schema.sh \
    scripts/stage-ch004-schema.sh \
    scripts/status-ch004-schema.sh \
    scripts/test-ch004-full.sh \
    scripts/test-ch004-schema.sh \
    scripts/vet-ch004-schema.sh

git diff --cached --check
git diff --cached --stat
