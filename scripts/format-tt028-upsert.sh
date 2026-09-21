#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSchema/materialized.go \
  hat/hatSchema/tt028_upsert_conflict_test.go \
  hat/hatSchema/tt028_upsert_conflict_benchmark_test.go
