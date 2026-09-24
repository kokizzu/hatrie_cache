#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatFiber/scheduler.go hat/hatFiber/tt033_tenant_quota.go hat/hatFiber/tt033_tenant_quota_baseline_benchmark_test.go hat/hatFiber/tt033_tenant_quota_benchmark_test.go hat/hatFiber/tt033_tenant_quota_test.go
