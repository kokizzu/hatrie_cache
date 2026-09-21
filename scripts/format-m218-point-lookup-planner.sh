#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/materialized_point_lookup_resolver.go \
  hat/hatSql/materialized_point_lookup_resolver_test.go \
  hat/hatSql/materialized_point_lookup_resolver_benchmark_test.go
