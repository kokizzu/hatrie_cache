#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatFiber/scheduler.go hat/hatFiber/tu30_scheduler_test.go hat/hatFiber/tu30_scheduler_benchmark_test.go
