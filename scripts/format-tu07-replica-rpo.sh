#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatCache/monitoring.go \
	hat/hatCache/replication_region.go \
	hat/hatCache/tu07_replica_rpo_metrics_test.go
