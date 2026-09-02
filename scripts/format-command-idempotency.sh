#!/usr/bin/env sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
gofmt -w \
        hat/hatCache/command_idempotency.go \
        hat/hatCache/command_idempotency_benchmark_test.go \
        hat/hatCache/command_idempotency_test.go \
	hat/hatCache/journal.go \
	hat/hatCache/journal_format.go \
	hat/hatCache/journal_wire.go \
	hat/hatCache/journal_wire_test.go \
	hat/hatCache/monitoring.go \
	hat/hatCommand/command.go \
	hat/hatCommand/idempotency_wire_test.go \
	hat/hatCommand/wire.go
	gofmt -w api.go cmd/hatrie-cache/main.go cmd/hatrie-cache/main_test.go
