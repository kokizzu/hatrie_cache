#!/bin/sh
set -eu

go test -run '^TestMonitoringSourceFrontier' ./hat/hatCache
go test ./hat/hatCache
go test -race ./hat/hatCache
git diff --check -- \
	README.md \
	INSPIRATION.md \
	Makefile \
	SOURCE_FRONTIER_MONITORING.md \
	hat/hatCache/monitoring.go \
	hat/hatCache/source_frontier_monitoring.go \
	hat/hat/hatCache/source_frontier_monitoring_test.go \
	scripts/inspect-frontier-metrics.sh \
	scripts/test-source-frontier-monitoring.sh \
	scripts/format-source-frontier-monitoring.sh \
	scripts/verify-source-frontier-monitoring.sh \
	scripts/deliver-source-frontier-monitoring.sh
