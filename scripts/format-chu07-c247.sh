#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatCache/async_command.go \
	hat/hatCache/chu07_mutation_status.go \
	hat/hatCache/chu07_mutation_lifecycle_test.go \
	hat/hatCache/chu07_mutation_lifecycle_benchmark_test.go \
	hat/hatCache/journal.go \
	hat/hatCache/system_tables.go
