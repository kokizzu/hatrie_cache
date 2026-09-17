#!/bin/sh
set -eu

gofmt -w \
	hat/hatCache/monitoring.go \
	hat/hatCache/replication.go \
	hat/hatCache/replication_test.go \
	hat/hatCache/tr050_replication_byte_backpressure_test.go \
	hat/hatReplication/model.go
