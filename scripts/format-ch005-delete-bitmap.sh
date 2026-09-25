#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
    hat/hatMerkle/delete_bitmap.go \
    hat/hatMerkle/part_manifest.go \
    hat/hatMerkle/part_catalog.go \
    hat/hatMerkle/part_catalog_persistence.go \
    hat/hatMerkle/ch005_delete_bitmap_manifest_test.go \
    hat/hatMerkle/ch005_delete_bitmap_manifest_benchmark_test.go \
    hat/hatSql/typed_table_patch_snapshot.go \
    hat/hatSql/ch005_patch_snapshot_benchmark_test.go \
    hat/hatSql/ch005_delete_bitmap_manifest_test.go
