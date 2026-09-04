#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --check
git diff --stat -- \
	BENCHMARK.md \
	INSPIRATION.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	JOURNAL_RETENTION.md \
	README.md \
	api.go \
	deploy/hatrie-cache.json \
	cmd/hatrie-cache/main.go \
	cmd/hatrie-cache/main_test.go \
	hat/hatCache/journal.go \
	hat/hatCache/journal_retention_benchmark_test.go \
	hat/hatCache/journal_retention_test.go \
	hat/hatCache/journal_segments.go \
	hat/hatCache/script_defaults_test.go \
	hat/hatJournal/journal.go \
	hat/hatJournal/journal_retention_test.go \
	journal_retention_api.go \
	scripts/monitoring-server.sh \
	scripts/format-journal-retention.sh \
	scripts/test-journal-retention.sh \
	scripts/test-race-journal-retention.sh \
	scripts/vet-journal-retention.sh \
	scripts/benchmark-journal-retention.sh
git diff -- \
	hat/hatJournal/journal.go \
	hat/hatCache/journal.go \
	hat/hatCache/journal_segments.go \
	cmd/hatrie-cache/main.go \
	cmd/hatrie-cache/main_test.go \
	scripts/monitoring-server.sh \
	deploy/hatrie-cache.json
