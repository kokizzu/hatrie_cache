#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  CH014B_MUTATION_DEPENDENCY_READY.md \
  hat/hatSql/mutation_dependency_graph.go \
  hat/hatSql/sql_mutation_dependency_queue.go \
  hat/hatSql/ch014b_mutation_dependency_graph_baseline_benchmark_test.go \
  hat/hatSql/ch014b_mutation_dependency_graph_benchmark_test.go \
  hat/hatSql/ch014b_mutation_dependency_graph_test.go \
  hat/hatSql/ch014b_mutation_dependency_ready.go \
  hat/hatSql/ch014b_mutation_dependency_ready_internal_test.go \
  scripts/benchmark-ch014b-mutation-dependency-graph-baseline.sh \
  scripts/benchmark-ch014b-mutation-dependency-graph.sh \
  scripts/commit-ch014b-mutation-dependency-graph.sh \
  scripts/format-ch014b-mutation-dependency-graph.sh \
  scripts/race-ch014b-mutation-dependency-graph.sh \
  scripts/review-ch014b-mutation-dependency-graph.sh \
  scripts/run-ch014b-mutation-dependency-graph-benchmark.sh \
  scripts/stage-ch014b-mutation-dependency-graph.sh \
  scripts/test-ch014b-mutation-dependency-graph.sh \
  scripts/verify-ch014b-mutation-dependency-graph.sh \
  scripts/push-ch014b-mutation-dependency-graph.sh
