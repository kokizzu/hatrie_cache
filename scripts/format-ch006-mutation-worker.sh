#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/ch006_mutation_worker_baseline_test.go \
  hat/hatSql/ch006_mutation_worker_test.go \
  hat/hatSql/sql_mutation_dependency_queue_worker.go
