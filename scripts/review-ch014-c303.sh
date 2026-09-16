#!/usr/bin/env bash
set -euo pipefail

paths=(
  hat/hatPipeline/mutation_dependency_graph.go
  hat/hatPipeline/mutation_dependency_graph_test.go
  hat/hatPipeline/mutation_dependency_graph_performance_test.go
  scripts/format-ch014-c302.sh
  scripts/test-race-ch014-c302.sh
  scripts/vet-ch014-c302.sh
  scripts/benchmark-ch014-c302.sh
  scripts/review-ch014-c303.sh
  scripts/stage-ch014-c303.sh
  scripts/inspect-staged-ch014-c303.sh
  scripts/commit-ch014-c303.sh
  scripts/push-ch014-c303.sh
  MUTATION_DEPENDENCY_GRAPH.md
  README.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
)

git diff --check -- "${paths[@]}"
git status --short -- "${paths[@]}" Makefile
git diff --stat -- "${paths[@]}"
