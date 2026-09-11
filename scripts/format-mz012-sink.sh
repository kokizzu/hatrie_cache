#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/journal_exactly_once_sink.go hat/hatCache/journal_exactly_once_test.go hat/hatCache/journal_exactly_once_benchmark_test.go
