#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/sql_mutation_dependency_queue.go \
  hat/hatSql/ch006_durable_mutation_queue_test.go \
  hat/hatSql/ch006_durable_mutation_queue_benchmark_test.go
