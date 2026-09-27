#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root_dir"
gofmt -w \
  hat/hatSql/contracts.go \
  hat/hatSql/catalog.go \
  hat/hatSql/session.go \
  hat/hatSql/ch002_physical_part_pruning.go \
  hat/hatSql/m090e_predicate_columnar_source.go \
  hat/hatSql/m090e_predicate_columnar_source_test.go
