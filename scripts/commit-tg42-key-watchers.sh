#!/bin/sh
set -eu

git add -- \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	Makefile \
	README.md \
	TT040_SPACE_CHANGEFEED.md \
	hat/hatCache/journal_key_watch_benchmark_test.go \
	hat/hatCache/journal_key_watch_test.go \
	hat/hatCache/journal_segments.go \
	hat/hatCache/journal_subscription.go \
	scripts/benchmark-tg42-key-watchers.sh \
	scripts/commit-tg42-key-watchers.sh \
	scripts/format-tg42-key-watchers.sh \
	scripts/push-tg42-key-watchers.sh \
	scripts/race-tg42-key-watchers.sh \
	scripts/review-tg42-key-watchers.sh \
	scripts/test-tg42-key-watchers.sh \
	scripts/test-tg42-package.sh \
	scripts/vet-tg42-key-watchers.sh
git diff --cached --check
git commit -m "feat: add journal key watchers"
