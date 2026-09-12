#!/bin/sh
set -eu

git status --short --branch
git diff --check -- Makefile BENCHMARK.md CLIENT_SDK.md INSPIRATION.md hat/hatMonitoring/client.go hat/hatMonitoring/client_command_test.go hat/hatMonitoring/client_command_benchmark_test.go scripts/benchmark-monitoring-command-client.sh scripts/format-monitoring-command-client.sh scripts/review-monitoring-command-batch.sh scripts/commit-monitoring-command-batch.sh scripts/push-monitoring-command-batch.sh scripts/race-monitoring-command-client.sh scripts/test-monitoring-command-client.sh scripts/vet-monitoring-command-client.sh
git diff --stat -- Makefile BENCHMARK.md CLIENT_SDK.md INSPIRATION.md hat/hatMonitoring/client.go hat/hatMonitoring/client_command_test.go hat/hatMonitoring/client_command_benchmark_test.go scripts/benchmark-monitoring-command-client.sh scripts/format-monitoring-command-client.sh scripts/review-monitoring-command-batch.sh scripts/commit-monitoring-command-batch.sh scripts/push-monitoring-command-batch.sh scripts/race-monitoring-command-client.sh scripts/test-monitoring-command-client.sh scripts/vet-monitoring-command-client.sh
