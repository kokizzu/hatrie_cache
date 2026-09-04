#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --check
git diff --stat -- \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	INSPIRATION.md \
	README.md \
	REPLAY_PROGRESS.md \
	cmd/hatrie-cache/main.go \
	hat/hatCache/journal.go \
	hat/hatCache/journal_replay_progress.go \
	hat/hatCache/journal_replay_progress_benchmark_test.go \
	hat/hatCache/journal_replay_progress_test.go \
	journal_replay_progress_api.go \
	scripts/benchmark-replay-progress.sh \
	scripts/format-replay-progress.sh \
	scripts/review-replay-progress.sh \
	scripts/test-replay-progress.sh \
	scripts/test-race-replay-progress.sh \
	scripts/vet-replay-progress.sh
git diff -- \
	hat/hatCache/journal.go \
	hat/hatCache/journal_replay_progress.go \
	hat/hatCache/journal_replay_progress_benchmark_test.go \
	hat/hatCache/journal_replay_progress_test.go
