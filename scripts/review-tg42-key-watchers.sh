#!/bin/sh
set -eu

git diff --check
git status --short
git status --short -- BENCHMARK.md
rg -n 'T-G42 Key Watchers|T-G42 Key-Watcher Benchmark' BENCHMARK.md TT040_SPACE_CHANGEFEED.md
git diff --stat -- README.md ADOPTED_QUERY_ENGINE_IDEAS.md TT040_SPACE_CHANGEFEED.md BENCHMARK.md Makefile hat/hatCache/journal_segments.go hat/hatCache/journal_subscription.go hat/hatCache/journal_key_watch_test.go hat/hatCache/journal_key_watch_benchmark_test.go scripts
git diff -- hat/hatCache/journal_segments.go hat/hatCache/journal_subscription.go TT040_SPACE_CHANGEFEED.md README.md ADOPTED_QUERY_ENGINE_IDEAS.md Makefile
sed -n '1,220p' hat/hatCache/journal_key_watch_test.go
sed -n '1,260p' hat/hatCache/journal_key_watch_benchmark_test.go
