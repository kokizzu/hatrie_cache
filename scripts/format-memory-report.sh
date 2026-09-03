#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatMonitoring/memory.go hat/hatMonitoring/memory_test.go hat/hatCache/monitoring.go hat/hatCache/monitoring_memory_test.go api.go
