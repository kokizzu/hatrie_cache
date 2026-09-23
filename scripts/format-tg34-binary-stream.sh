#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatHttp/binary_stream.go hat/hatHttp/binary_stream_test.go hat/hatHttp/binary_stream_benchmark_test.go
