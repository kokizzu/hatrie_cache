#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSchema/materialized.go hat/hatSchema/tt025_online_uniqueness_test.go hat/hatSchema/tt025_online_uniqueness_benchmark_test.go
