#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/snapshot_export_resume.go hat/hatCache/snapshot_export_resume_test.go hat/hatCache/snapshot_export_resume_benchmark_test.go hat/hatSql/explain_arrangement.go hat/hatSql/m_u05_arrangement_recovery.go
