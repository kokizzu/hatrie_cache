#!/usr/bin/env bash
set -euo pipefail

git diff --check -- \
    ADOPTED_QUERY_ENGINE_IDEAS.md \
    BENCHMARK.md \
    ENGINE_IDEAS.md \
    MZ032_LATE_DATA_RECLOCK.md \
    README.md \
    hat/hatPipeline/mz032_late_data_reclock.go \
    hat/hatPipeline/mz032_late_data_reclock_test.go \
    hat/hatPipeline/mz032_late_data_reclock_benchmark_test.go

printf '%s\n' 'MZ-032 references:'
rg -n 'MZ-032|MZ032|LateDataReclock' \
    ADOPTED_QUERY_ENGINE_IDEAS.md \
    BENCHMARK.md \
    ENGINE_IDEAS.md \
    MZ032_LATE_DATA_RECLOCK.md \
    README.md \
    hat/hatPipeline/mz032_late_data_reclock.go \
    hat/hatPipeline/mz032_late_data_reclock_test.go \
    hat/hatPipeline/mz032_late_data_reclock_benchmark_test.go

printf '%s\n' 'MZ-032 paths:'
git status --short -- \
    ADOPTED_QUERY_ENGINE_IDEAS.md \
    BENCHMARK.md \
    ENGINE_IDEAS.md \
    MZ032_LATE_DATA_RECLOCK.md \
    README.md \
    hat/hatPipeline/mz032_late_data_reclock.go \
    hat/hatPipeline/mz032_late_data_reclock_test.go \
    hat/hatPipeline/mz032_late_data_reclock_benchmark_test.go \
    scripts/benchmark-mz032-late-data-reclock.sh \
    scripts/commit-mz032-late-data-reclock.sh \
    scripts/format-mz032-late-data-reclock.sh \
    scripts/inspect-authoritative-next-idea.sh \
    scripts/inspect-mz032-c203.sh \
    scripts/push-mz032-late-data-reclock.sh \
    scripts/race-mz032-late-data-reclock.sh \
    scripts/review-mz032-late-data-reclock.sh \
    scripts/test-mz032-late-data-reclock.sh \
    scripts/verify-mz032-late-data-reclock.sh \
    scripts/vet-mz032-late-data-reclock.sh
