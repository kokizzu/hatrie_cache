#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatStorage/compaction_metrics.go hat/hatStorage/compaction_diagnostics.go hat/hatStorage/m_u39_compaction_metrics_test.go hat/hatStorage/compaction_metrics_c239_benchmark_test.go
