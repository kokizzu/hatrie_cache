#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatDataStructure/versioned_migration_manager.go \
	hat/hatDataStructure/tu21_versioned_migration_manager_test.go \
	hat/hatDataStructure/tu21_versioned_migration_manager_benchmark_test.go
