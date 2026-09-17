#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSql/catalog.go hat/hatSql/mu014_object_dependency_catalog.go hat/hatSql/mu014_object_dependency_catalog_baseline_benchmark_test.go hat/hatSql/mu014_object_dependency_catalog_test.go hat/hatSql/mu014_object_dependency_catalog_benchmark_test.go
