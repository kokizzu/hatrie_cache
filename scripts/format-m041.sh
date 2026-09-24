#!/usr/bin/env bash
set -euo pipefail

	gofmt -w \
	hat/hatSql/m_u05_arrangement_recovery.go \
	hat/hatSql/m_u05_arrangement_recovery_bundle.go \
	hat/hatSql/m_u05_arrangement_recovery_bundle_test.go \
	hat/hatSql/m_u05_arrangement_recovery_bundle_benchmark_test.go
