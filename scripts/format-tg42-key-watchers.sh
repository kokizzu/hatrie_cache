#!/bin/sh
set -eu

gofmt -w \
	hat/hatCache/journal_subscription.go \
	hat/hatCache/journal_segments.go \
	hat/hatCache/journal_key_watch_test.go \
	hat/hatCache/journal_key_watch_benchmark_test.go
