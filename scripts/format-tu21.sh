#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSchema/tu21_space_migration.go \
  hat/hatSchema/tu21_space_migration_test.go \
  hat/hatSchema/tu21_space_migration_baseline_benchmark_test.go \
  hat/hatSchema/tu21_space_migration_benchmark_test.go
