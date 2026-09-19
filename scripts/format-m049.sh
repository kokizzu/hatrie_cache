#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/m049_catalog_migration.go hat/hatSql/m049_catalog_migration_test.go hat/hatSql/m049_catalog_migration_benchmark_test.go hat/hatSql/m049_catalog_migration_baseline_benchmark_test.go
