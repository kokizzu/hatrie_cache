#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatAuth/rbac.go hat/hatAuth/role_catalog.go hat/hatAuth/tr048_function_grants_test.go
