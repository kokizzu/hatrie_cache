#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatTopology/config_watch.go hat/hatTopology/config_watch_index_test.go hat/hatTopology/config_watch_index_benchmark_test.go
