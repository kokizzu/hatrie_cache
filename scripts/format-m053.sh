#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

gofmt -w \
  hat/hatSql/m_u03_external_snapshot_ingestion.go \
  hat/hatSql/m_u03_external_snapshot_ingestion_test.go \
  hat/hatSql/m_u03_external_snapshot_ingestion_benchmark_test.go
