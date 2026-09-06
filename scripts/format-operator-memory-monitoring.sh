#!/bin/sh
set -eu

gofmt -w \
	hat/hatCache/monitoring.go \
	hat/hatCache/operator_memory_monitoring.go \
	hat/hatCache/operator_memory_monitoring_test.go
