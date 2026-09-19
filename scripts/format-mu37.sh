#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatStorage/compaction_diagnostics.go \
	hat/hatStorage/m_u37_compaction_diagnostics_test.go \
	hat/hatStorage/m_u37_compaction_diagnostics_benchmark_test.go \
	hat/hatStorage/m_u37_compaction_diagnostics_baseline_benchmark_test.go
