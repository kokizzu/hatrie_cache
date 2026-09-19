#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

gofmt -w \
  hat/hatSql/catalog_schema_evolution.go \
  hat/hatSql/catalog_schema_evolution_test.go \
  hat/hatSql/catalog_schema_evolution_benchmark_test.go
