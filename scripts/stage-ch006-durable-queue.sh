#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CH006_DURABLE_MUTATION_QUEUE.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  hat/hatSql/sql_mutation_dependency_queue.go \
  hat/hatSql/ch006_durable_mutation_queue_test.go \
  hat/hatSql/ch006_durable_mutation_queue_benchmark_test.go \
  scripts/benchmark-ch006-durable-queue-baseline.sh \
  scripts/benchmark-ch006-durable-queue.sh \
  scripts/format-ch006-durable-queue.sh \
  scripts/review-ch006-durable-queue.sh \
  scripts/stage-ch006-durable-queue.sh \
  scripts/test-ch006-durable-queue.sh \
  scripts/commit-ch006-durable-queue.sh \
  scripts/push-ch006-durable-queue.sh

git diff --cached --check
