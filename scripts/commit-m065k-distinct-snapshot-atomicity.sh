#!/usr/bin/env bash
set -eu

git add -- \
    Makefile INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md \
    INCREMENTAL_DISTINCT_FRAME_WINDOW.md \
    hat/hatSql/incremental_frame_window.go \
    hat/hatSql/m065k_distinct_snapshot_atomicity_test.go \
    scripts/format-m065k-distinct-snapshot-atomicity.sh \
    scripts/test-m065k-distinct-snapshot-atomicity.sh \
    scripts/test-race-m065k-distinct-snapshot-atomicity.sh \
    scripts/vet-m065k-distinct-snapshot-atomicity.sh \
    scripts/review-m065k-distinct-snapshot-atomicity.sh \
    scripts/commit-m065k-distinct-snapshot-atomicity.sh \
    scripts/push-m065k-distinct-snapshot-atomicity.sh
git diff --cached --check
git commit -m "perf(sql): reuse validated distinct frame state"
