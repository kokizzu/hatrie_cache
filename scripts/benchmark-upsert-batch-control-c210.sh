#!/usr/bin/env bash
set -euo pipefail

source_backup="$(mktemp)"
restore_source() {

  cp "$source_backup" hat/hatDataStructure/upsert_batch.go
  rm -f "$source_backup"
}
trap restore_source EXIT

cp hat/hatDataStructure/upsert_batch.go "$source_backup"
printf '%s\n' '--- baseline ---'
git show "${BASELINE_REF:-HEAD^}:hat/hatDataStructure/upsert_batch.go" > hat/hatDataStructure/upsert_batch.go
go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkUpsertBatchSmallVectorC210/batch_(32|64|1000)$' -benchtime=1s -benchmem -count=3
printf '%s\n' '--- final ---'
cp "$source_backup" hat/hatDataStructure/upsert_batch.go
go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkUpsertBatchSmallVectorC210/batch_(32|64|1000)$' -benchtime=1s -benchmem -count=3
