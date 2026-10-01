#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench 'BenchmarkCommandJournalSpace(FeedNextAck|SubscriptionNext)$' -benchmem -count=5
