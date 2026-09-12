#!/bin/sh
set -eu

git diff --check
git status --short
git diff --stat -- Makefile INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md JOURNAL_REPLAY.md BENCHMARK.md hat/hatCache/journal.go hat/hatCache/journal_segments.go hat/hatCache/journal_replay_metadata_test.go scripts/benchmark-journal-replay.sh scripts/test-journal-replay.sh scripts/format-journal-replay.sh scripts/review-journal-replay.sh scripts/commit-journal-replay.sh scripts/push-journal-replay.sh
