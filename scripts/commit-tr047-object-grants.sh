#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md INSPIRATION_BACKLOG.md TR047_OBJECT_GRANTS.md \
  hat/hatAuth/rbac.go hat/hatAuth/tr047_object_grants_test.go \
  hat/hatAuth/tr047_object_grants_benchmark_test.go \
  hat/hatCache/monitoring.go hat/hatCache/grpc.go \
  hat/hatCache/tr047_object_grants_test.go \
  scripts/test-tr047-object-grants.sh scripts/format-tr047-object-grants.sh \
  scripts/benchmark-tr047-object-grants.sh scripts/race-tr047-object-grants.sh \
  scripts/vet-tr047-object-grants.sh scripts/review-tr047-object-grants.sh \
  scripts/commit-tr047-object-grants.sh scripts/push-tr047-object-grants.sh
git commit -m "Add object-scoped RBAC grants"
