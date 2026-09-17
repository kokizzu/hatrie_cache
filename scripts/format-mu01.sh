#!/bin/sh
set -eu

gofmt -w hat/hatPipeline/connector_snapshot.go hat/hatPipeline/mu01_connector_snapshot_test.go hat/hatPipeline/mu01_connector_snapshot_benchmark_test.go
