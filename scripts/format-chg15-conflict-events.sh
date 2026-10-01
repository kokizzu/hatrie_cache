#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatReplication/conflict_event_log.go \
	hat/hatReplication/conflict_event_log_test.go \
	hat/hatReplication/conflict_event_log_benchmark_test.go
