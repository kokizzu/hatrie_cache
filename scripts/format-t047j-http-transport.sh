#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatCache/http_cluster_write_commit.go \
	hat/hatCache/tu47_cluster_write_commit_http_test.go \
	hat/hatCache/tu47_cluster_write_commit_http_benchmark_test.go
