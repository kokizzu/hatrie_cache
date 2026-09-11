#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache \
  -run '^$' \
  -bench 'BenchmarkCommandJournal(TailReplay100|SubscriptionReplay100|ExecuteCommandNoSubscription|SubscriptionLive)$' \
  -benchmem \
  -count=5
