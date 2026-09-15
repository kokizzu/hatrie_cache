#!/usr/bin/env bash
set -euo pipefail

source_backup="$(mktemp)"
restore_source() {

  cp "$source_backup" hat/hatDataStructure/upsert_batch.go
  rm -f "$source_backup"
}
trap restore_source EXIT

cp hat/hatDataStructure/upsert_batch.go "$source_backup"
git show "${BASELINE_REF:-HEAD^}:hat/hatDataStructure/upsert_batch.go" > hat/hatDataStructure/upsert_batch.go
go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkUpsertBatchSmallVectorFreshC210$' -benchmem -count=3
