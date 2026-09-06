#!/bin/sh
set -eu

go test -run '^TestMonitoringOperatorMemoryMetrics' ./hat/hatCache
go test ./hat/hatCache
go test -race ./hat/hatCache
git diff --check -- \
	README.md \
	INSPIRATION.md \
	Makefile \
	OPERATOR_MEMORY_MONITORING.md \
	hat/hatCache/monitoring.go \
	hat/hatCache/operator_memory_monitoring.go \
	hat/hatCache/operator_memory_monitoring_test.go \
	scripts/inspect-frontier-metrics.sh \
	scripts/test-operator-memory-monitoring.sh \
	scripts/format-operator-memory-monitoring.sh \
	scripts/verify-operator-memory-monitoring.sh \
	scripts/deliver-operator-memory-monitoring.sh
