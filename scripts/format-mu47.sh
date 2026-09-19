#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/m_u47_progress_frame.go \
	hat/hatSql/m_u47_progress_frame_test.go \
	hat/hatSql/m_u47_progress_frame_baseline_benchmark_test.go \
	hat/hatSql/m_u47_progress_frame_benchmark_test.go
