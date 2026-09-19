#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatIndexStats/tracker.go hat/hatIndexStats/tracker_test.go hat/hatIndexStats/tracker_benchmark_test.go
