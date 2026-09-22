#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/c238_mutation_queue_progress_test.go hat/hatSql/c238_mutation_queue_progress_baseline_benchmark_test.go hat/hatSql/c238_mutation_queue_progress_benchmark_test.go
