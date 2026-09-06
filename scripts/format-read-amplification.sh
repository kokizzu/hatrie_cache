#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
gofmt -w hat/hatMetrics/read_amplification.go hat/hatMetrics/read_amplification_test.go
