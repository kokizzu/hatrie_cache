#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/materialized.go \
	hat/hatSql/materialized_hydration.go \
	hat/hatSql/materialized_point_lookup.go \
	hat/hatSql/materialized_point_lookup_build.go \
	hat/hatSql/materialized_point_lookup_retirement.go \
	hat/hatSql/m223_materialized_hydration_test.go \
	hat/hatSql/m223_materialized_hydration_benchmark_test.go
