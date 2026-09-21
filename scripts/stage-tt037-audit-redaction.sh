#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  TT037_AUDIT_REDACTION.md \
  hat/hatAudit/audit.go \
  hat/hatAudit/tt037_audit_redaction_benchmark_test.go \
  hat/hatAudit/tt037_audit_redaction_test.go \
  scripts/benchmark-tt037-audit-redaction.sh \
  scripts/commit-tt037-audit-redaction.sh \
  scripts/format-tt037-audit-redaction.sh \
  scripts/push-tt037-audit-redaction.sh \
  scripts/race-tt037-audit-redaction.sh \
  scripts/stage-tt037-audit-redaction.sh \
  scripts/test-tt037-audit-redaction.sh \
  scripts/verify-tt037-audit-redaction.sh

git diff --cached --check
git status --short
