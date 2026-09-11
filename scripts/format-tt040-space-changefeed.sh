#!/bin/sh
set -eu

gofmt -w \
  hat/hatCache/journal_subscription.go \
  hat/hatCache/journal_segments.go \
  hat/hatCache/journal_space_subscription_test.go \
  hat/hatCache/journal_space_subscription_benchmark_test.go
