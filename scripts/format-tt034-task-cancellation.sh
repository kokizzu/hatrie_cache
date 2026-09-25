#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatFiber/scheduler.go hat/hatFiber/tt034_task_cancellation.go hat/hatFiber/tt034_task_cancellation_test.go
