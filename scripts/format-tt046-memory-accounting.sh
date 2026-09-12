#!/bin/sh
set -eu

gofmt -w \
	hat/hatCache/memory_accounting.go \
	hat/hatCache/memory_compaction.go \
	hat/hatCache/monitoring.go \
	hat/hatCache/tt046_memory_accounting_baseline_test.go \
	hat/hatCache/tt046_memory_accounting_test.go
