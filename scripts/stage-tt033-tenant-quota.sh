#!/usr/bin/env bash
set -euo pipefail

git add \
	ENGINE_IDEAS.md \
	Makefile \
	TT033_FIBER_QUOTAS.md \
	hat/hatFiber/scheduler.go \
	hat/hatFiber/tt033_tenant_quota.go \
	hat/hatFiber/tt033_tenant_quota_baseline_benchmark_test.go \
	hat/hatFiber/tt033_tenant_quota_benchmark_test.go \
	hat/hatFiber/tt033_tenant_quota_test.go \
	scripts/benchmark-tt033-tenant-quota.sh \
	scripts/format-tt033-tenant-quota.sh \
	scripts/stage-tt033-tenant-quota.sh \
	scripts/commit-tt033-tenant-quota.sh \
	scripts/push-tt033-tenant-quota.sh \
	scripts/test-tt033-tenant-quota.sh \
	scripts/verify-tt033-tenant-quota.sh
