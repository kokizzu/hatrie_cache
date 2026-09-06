#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatMetrics/collection_metrics.go hat/hatMetrics/collection_metrics_test.go
