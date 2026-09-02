#!/bin/sh
set -eu

gofmt -w api.go cmd/hatrie-cache/main.go cmd/hatrie-cache/async_config_test.go hat/hatCache/async_command.go hat/hatCache/async_command_http.go hat/hatCache/async_command_http_benchmark_test.go hat/hatCache/async_command_http_test.go hat/hatCache/async_command_test.go hat/hatCache/journal.go hat/hatCache/monitoring.go
