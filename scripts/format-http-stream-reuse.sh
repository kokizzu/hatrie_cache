#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatHttp/binary_stream.go hat/hatHttp/binary_stream_reuse_test.go hat/hatHttp/binary_stream_reuse_benchmark_test.go
