#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	./hat/hatSql/ch004_final_schema_registry.go \
	./hat/hatSql/ch004_final_schema_registry_test.go \
	./hat/hatSql/ch004_final_schema_registry_baseline_benchmark_test.go
