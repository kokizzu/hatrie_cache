#!/usr/bin/env bash
set -euo pipefail

gofmt -w api.go hat/hatAudit/audit.go hat/hatAudit/tr048_audit_sampling_test.go hat/hatAudit/tr048_audit_sampling_benchmark_test.go hat/hatCache/audit.go hat/hatCache/tr048_audit_sampling_test.go cmd/hatrie-cache/main.go cmd/hatrie-cache/tr048_audit_sampling_test.go
