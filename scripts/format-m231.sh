#!/bin/sh
set -eu

gofmt -w \
  hat/hatCache/journal_exactly_once_upsert.go \
  hat/hatCache/m231_exactly_once_upsert_test.go \
  hat/hatCache/m231_exactly_once_upsert_benchmark_test.go
