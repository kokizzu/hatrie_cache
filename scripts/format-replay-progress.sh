#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatCache/journal.go \
	hat/hatCache/journal_replay_progress.go \
	hat/hatCache/journal_replay_progress_test.go \
	hat/hatCache/journal_replay_progress_benchmark_test.go \
	journal_replay_progress_api.go
