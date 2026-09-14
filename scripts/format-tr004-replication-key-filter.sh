#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatCache/replication.go \
	hat/hatCache/replication_digest.go \
	hat/hatCache/replication_key_filter.go \
	hat/hatCache/tr004_replication_key_filter_test.go \
	hat/hatCache/tr004_replication_key_filter_benchmark_test.go \
	cmd/hatrie-cache/main.go \
	cmd/hatrie-cache/tr004_replication_key_filter_config_test.go
