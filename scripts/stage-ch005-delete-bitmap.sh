#!/usr/bin/env bash
set -euo pipefail

git add -- \
    BENCHMARK.md \
    CH005_DELETE_BITMAP_SNAPSHOT.md \
    ENGINE_IDEAS.md \
    Makefile \
    hat/hatMerkle/delete_bitmap.go \
    hat/hatMerkle/part_manifest.go \
    hat/hatMerkle/part_catalog.go \
    hat/hatMerkle/part_catalog_persistence.go \
    hat/hatMerkle/ch005_delete_bitmap_manifest_test.go \
    hat/hatMerkle/ch005_delete_bitmap_manifest_benchmark_test.go \
    hat/hatSql/typed_table_patch_snapshot.go \
    hat/hatSql/ch005_delete_bitmap_manifest_test.go \
    hat/hatSql/ch005_patch_snapshot_benchmark_test.go \
    scripts/benchmark-ch005-delete-bitmap-baseline.sh \
    scripts/benchmark-ch005-delete-bitmap.sh \
    scripts/format-ch005-delete-bitmap.sh \
    scripts/race-ch005-delete-bitmap.sh \
    scripts/test-ch005-delete-bitmap-package.sh \
    scripts/test-ch005-delete-bitmap.sh \
    scripts/vet-ch005-delete-bitmap.sh \
    scripts/stage-ch005-delete-bitmap.sh \
    scripts/commit-ch005-delete-bitmap.sh \
    scripts/push-ch005-delete-bitmap.sh
git diff --cached --check
