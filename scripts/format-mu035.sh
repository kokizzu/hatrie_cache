#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/m_u35_snapshot_blocking.go hat/hatSql/m_u35_snapshot_blocking_test.go hat/hatSql/m_u35_snapshot_blocking_benchmark_test.go
