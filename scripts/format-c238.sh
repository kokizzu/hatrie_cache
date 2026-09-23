#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/mutation_controller.go hat/hatSql/c238_mutation_progress_test.go hat/hatSql/c238_mutation_progress_benchmark_test.go
