#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
gofmt -w hat/hatMetrics/codec_metrics.go hat/hatMetrics/codec_metrics_test.go
