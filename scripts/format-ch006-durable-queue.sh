#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/sql_mutation_dependency_queue.go \
  hat/hatSql/sql_mutation_dependency_queue_lease_unix.go \
  hat/hatSql/sql_mutation_dependency_queue_lease_other.go \
  hat/hatSql/ch006_durable_mutation_queue_test.go \
  hat/hatSql/ch006_durable_mutation_queue_benchmark_test.go
