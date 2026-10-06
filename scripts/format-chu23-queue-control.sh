#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatCache/async_command.go \
	hat/hatCache/async_command_queue.go \
	hat/hatCache/async_command_queue_http.go \
	hat/hatCache/chu23_async_command_queue_test.go \
	hat/hatCache/chu23_async_command_queue_benchmark_test.go \
	hat/hatSql/codex_chu23_compile_shim.go
