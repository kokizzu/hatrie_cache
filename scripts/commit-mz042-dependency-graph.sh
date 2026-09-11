#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md MATERIALIZED_VIEW_DEPENDENCY_GRAPH.md hat/hatSql/materialized.go hat/hatSql/materialized_dependency_graph_test.go hat/hatSql/materialized_dependency_benchmark_test.go scripts/benchmark-mz042-baseline.sh scripts/benchmark-mz042-dependency-graph.sh scripts/commit-mz042-dependency-graph.sh scripts/format-mz042-dependency-graph.sh scripts/push-mz042-dependency-graph.sh scripts/review-mz042-dependency-graph.sh scripts/test-mz042-dependency-graph.sh scripts/test-race-mz042-dependency-graph.sh scripts/verify-mz042-docs.sh scripts/vet-mz042-dependency-graph.sh
git commit -m 'hatSql: index materialized view dependencies'
