#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/tu34_space_sync_policy.go hat/hatCache/tu34_space_sync_policy_test.go hat/hatCache/tu34_space_sync_policy_benchmark_test.go hat/hatCache/journal.go
