#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/space_changefeed.go hat/hatCache/tu39_space_changefeed_test.go hat/hatCache/tu39_space_changefeed_benchmark_test.go
