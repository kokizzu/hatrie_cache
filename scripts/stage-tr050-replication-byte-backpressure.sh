#!/bin/sh
set -eu

git add \
	BENCHMARK.md \
	INSPIRATION_BACKLOG.md \
	Makefile \
	README.md \
	TR050_REPLICATION_BYTE_BACKPRESSURE.md \
	hat/hatCache/monitoring.go \
	hat/hatCache/replication.go \
	hat/hatCache/replication_test.go \
	hat/hatCache/tr050_replication_byte_backpressure_test.go \
	hat/hatReplication/model.go \
	scripts/benchmark-tr050-replication-byte-backpressure.sh \
	scripts/commit-tr050-replication-byte-backpressure.sh \
	scripts/format-tr050-replication-byte-backpressure.sh \
	scripts/race-tr050-replication-byte-backpressure.sh \
	scripts/review-tr050-replication-byte-backpressure.sh \
	scripts/push-tr050-replication-byte-backpressure.sh \
	scripts/stage-tr050-replication-byte-backpressure.sh \
	scripts/test-tr050-replication-byte-backpressure.sh \
	scripts/test-tr050-replication-package.sh \
	scripts/verify-delivery-tr050-replication-byte-backpressure.sh \
	scripts/vet-tr050-replication-byte-backpressure.sh
