#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatAudit/audit.go \
  hat/hatAudit/tt037_audit_redaction_test.go \
  hat/hatAudit/tt037_audit_redaction_benchmark_test.go
