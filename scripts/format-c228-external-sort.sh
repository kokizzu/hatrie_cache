#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/ch228_external_sort_stability_test.go hat/hatSql/ch228_external_sort_stability_benchmark_test.go
