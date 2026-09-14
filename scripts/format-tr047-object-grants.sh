#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatAuth/rbac.go hat/hatAuth/tr047_object_grants_test.go hat/hatAuth/tr047_object_grants_benchmark_test.go hat/hatCache/tr047_object_grants_test.go
