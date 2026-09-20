#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatTopology/tu14_bucket_migration.go \
	hat/hatTopology/tu14_bucket_migration_test.go \
	hat/hatTopology/tu14_bucket_migration_contract_test.go \
	hat/hatTopology/tu14_bucket_migration_benchmark_test.go
