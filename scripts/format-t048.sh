#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatReplication/t048_remote_call.go hat/hatReplication/t048_remote_call_test.go hat/hatReplication/t048_remote_call_benchmark_test.go hat/hatReplication/t048_remote_call_baseline_benchmark_test.go
