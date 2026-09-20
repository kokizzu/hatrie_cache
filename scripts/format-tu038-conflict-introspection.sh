#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/tu038_conflict_introspection.go \
  hat/hatReplication/tu038_conflict_introspection_test.go \
  hat/hatReplication/tu038_conflict_introspection_benchmark_test.go \
  hat/hatReplication/conflict_policy.go
