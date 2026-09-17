#!/bin/sh
set -eu

git add -- \
	BENCHMARK.md \
	CH014_MUTATION_DEPENDENCY_GRAPH.md \
	INSPIRATION_BACKLOG.md \
	Makefile \
	README.md \
	hat/hatSql/mutation_dependency_graph.go \
	hat/hatSql/mutation_dependency_graph_benchmark_test.go \
	hat/hatSql/mutation_dependency_graph_test.go \
	scripts/benchmark-ch14.sh \
	scripts/commit-ch14.sh \
	scripts/format-ch14.sh \
	scripts/push-ch14.sh \
	scripts/review-ch14-staged.sh \
	scripts/review-ch14.sh \
	scripts/stage-ch14.sh \
	scripts/test-ch14.sh \
	scripts/verify-ch14.sh
