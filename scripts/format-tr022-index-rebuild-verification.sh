#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/index_rebuild_queue.go hat/hatSql/tr022_index_rebuild_verification_test.go hat/hatSql/tr022_index_rebuild_verification_public_test.go hat/hatSql/tr022_index_rebuild_verification_benchmark_test.go
