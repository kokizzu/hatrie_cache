#!/bin/sh
set -eu

git add -- \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	Makefile \
	README.md \
	TT043_MAINTENANCE_READ_ONLY.md \
	cmd/hatrie-cache/main.go \
	cmd/hatrie-cache/tt043_maintenance_read_only_test.go \
	hat/hatCache/grpc.go \
	hat/hatCache/maintenance_read_only.go \
	hat/hatCache/monitoring.go \
	hat/hatCache/tt043_maintenance_read_only_test.go \
	scripts/benchmark-tt043-maintenance-read-only.sh \
	scripts/commit-tt043-maintenance-read-only.sh \
	scripts/format-tt043-maintenance-read-only.sh \
	scripts/push-tt043-maintenance-read-only.sh \
	scripts/review-tt043-maintenance-read-only.sh \
	scripts/test-race-tt043-maintenance-read-only.sh \
	scripts/test-tt043-maintenance-read-only.sh \
	scripts/test-tt043-package.sh \
	scripts/verify-tt043-docs.sh \
	scripts/vet-tt043-maintenance-read-only.sh
git diff --cached --check
git diff --cached --stat
git commit -m "feat: add maintenance read-only mode"
