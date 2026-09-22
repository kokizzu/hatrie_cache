#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatJournal/journal.go \
  hat/hatCache/journal.go \
  hat/hatCache/journal_segments.go \
  hat/hatCache/journal_replica_retention.go \
  hat/hatCache/t212_replica_retention_test.go \
  hat/hatCache/t212_replica_retention_baseline_benchmark_test.go \
  hat/hatCache/t212_replica_retention_benchmark_test.go
