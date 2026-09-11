#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatCache/journal_source_checkpoint.go \
	hat/hatCache/journal_source_checkpoint_test.go \
	hat/hatCache/journal_source_checkpoint_benchmark_test.go
