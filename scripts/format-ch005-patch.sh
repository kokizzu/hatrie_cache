#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/ch005_patch_snapshot_test.go \
	hat/hatSql/ch005_patch_snapshot_benchmark_test.go \
	hat/hatSql/typed_table_patch_snapshot.go
