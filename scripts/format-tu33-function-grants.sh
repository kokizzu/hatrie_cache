#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatAuth/rbac.go hat/hatAuth/role_catalog.go hat/hatAuth/tu33_function_grants_test.go hat/hatAuth/tu33_function_grants_benchmark_test.go
