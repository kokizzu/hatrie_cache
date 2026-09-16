#!/bin/sh
set -eu

git add \
    ADOPTED_QUERY_ENGINE_IDEAS.md \
    BENCHMARK.md \
    CHU50_PART_WAL_CONSISTENCY.md \
    Makefile \
    PRODUCT_IDEA_GAPS.md \
    README.md \
    hat/hatBackup/chain.go \
    hat/hatBackup/consistency.go \
    hat/hatBackup/model.go \
    hat/hatCache/backup_bundle.go \
    hat/hatCache/backup_doctor.go \
    hat/hatCache/backup_partition_restore_test.go \
    hat/hatCache/backup_repository.go \
    hat/hatCache/backup_restore.go \
    hat/hatCache/ch_u50_backup_consistency_benchmark_test.go \
    hat/hatCache/ch_u50_backup_consistency_test.go \
    scripts/benchmark-chu50-c203.sh \
    scripts/check-chu50-c203.sh \
    scripts/commit-chu50-c203.sh \
    scripts/format-chu50-c203.sh \
    scripts/prepare-chu50-benchmark-baseline-c203.sh \
    scripts/push-chu50-c203.sh \
    scripts/race-chu50-c203.sh \
    scripts/stage-chu50-c203.sh \
    scripts/sync-chu50-benchmark-baseline-c203.sh \
    scripts/test-chu50-c203.sh \
    scripts/test-chu50-red-c203.sh \
    scripts/verify-chu50-docs-c203.sh \
    scripts/vet-chu50-c203.sh
