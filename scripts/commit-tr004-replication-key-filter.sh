#!/usr/bin/env bash
set -euo pipefail

git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	INSPIRATION_BACKLOG.md \
	Makefile \
	README.md \
	TR004_REPLICATION_KEY_FILTER.md \
	cmd/hatrie-cache/main.go \
	cmd/hatrie-cache/tr004_replication_key_filter_config_test.go \
	hat/hatCache/replication.go \
	hat/hatCache/replication_digest.go \
	hat/hatCache/replication_key_filter.go \
	hat/hatCache/tr004_replication_key_filter_benchmark_test.go \
	hat/hatCache/tr004_replication_key_filter_test.go \
 scripts/benchmark-tr004-replication-key-filter.sh \
 scripts/commit-tr004-replication-key-filter.sh \
 scripts/format-tr004-replication-key-filter.sh \
	scripts/monitoring-server.sh \
	scripts/push-tr004-replication-key-filter.sh \
	scripts/race-tr004-replication-key-filter.sh \
	scripts/review-tr004-replication-key-filter.sh \
	scripts/test-tr004-replication-key-filter.sh \
	scripts/vet-tr004-replication-key-filter.sh
git diff --cached --check
git commit -m "Add replication key-prefix filters"
