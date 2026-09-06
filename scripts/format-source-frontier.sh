#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatMetrics/source_frontier.go hat/hatMetrics/source_frontier_test.go
