#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md INSPIRATION_BACKLOG.md TR048_AUDIT_SAMPLING.md \
  api.go cmd/hatrie-cache/main.go cmd/hatrie-cache/tr048_audit_sampling_test.go \
  hat/hatAudit/audit.go hat/hatAudit/tr048_audit_sampling_test.go \
  hat/hatAudit/tr048_audit_sampling_benchmark_test.go \
  hat/hatCache/audit.go hat/hatCache/tr048_audit_sampling_test.go \
  scripts/monitoring-server.sh scripts/test-tr048-audit-sampling.sh \
  scripts/format-tr048-audit-sampling.sh scripts/benchmark-tr048-audit-sampling.sh \
  scripts/race-tr048-audit-sampling.sh scripts/vet-tr048-audit-sampling.sh \
  scripts/review-tr048-audit-sampling.sh scripts/commit-tr048-audit-sampling.sh \
  scripts/push-tr048-audit-sampling.sh
git commit -m "Add audit sampling and export sinks"
