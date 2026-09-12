#!/bin/sh
set -eu

git add Makefile BENCHMARK.md CLIENT_SDK.md INSPIRATION.md hat/hatMonitoring/client.go hat/hatMonitoring/client_command_test.go hat/hatMonitoring/client_command_benchmark_test.go scripts/test-monitoring-command-client.sh scripts/format-monitoring-command-client.sh scripts/benchmark-monitoring-command-client.sh scripts/vet-monitoring-command-client.sh scripts/race-monitoring-command-client.sh scripts/review-monitoring-command-client.sh scripts/commit-monitoring-command-client.sh scripts/push-monitoring-command-client.sh
git diff --cached --check
git commit -m "feat: add public HTTP command client"
