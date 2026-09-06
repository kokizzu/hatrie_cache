#!/bin/sh
set -eu

gofmt -w \
	hat/hatCache/monitoring.go \
	hat/hatCache/source_frontier_monitoring.go \
	hat/hatCache/source_frontier_monitoring_test.go
