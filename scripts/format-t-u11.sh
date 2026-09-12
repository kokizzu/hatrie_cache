#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatReplication/conflict_policy.go hat/hatReplication/conflict_policy_registry_test.go hat/hatReplication/conflict_policy_registry_benchmark_test.go
