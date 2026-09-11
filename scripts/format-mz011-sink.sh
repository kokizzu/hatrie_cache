#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/journal_sink.go hat/hatCache/journal_sink_test.go hat/hatCache/journal_sink_benchmark_test.go
