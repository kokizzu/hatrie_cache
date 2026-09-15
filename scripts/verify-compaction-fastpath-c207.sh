#!/usr/bin/env bash
set -euo pipefail

files=(
	hat/hatStorage/compaction_scheduler.go
	hat/hatStorage/compaction_scheduler_fastpath_test.go
)
go test "${files[@]}" -count=1
go test -race "${files[@]}" -count=1
go vet "${files[@]}"
