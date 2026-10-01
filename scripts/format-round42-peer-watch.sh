#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPeer/compact_config_watch.go \
  hat/hatPeer/compact_config_watch_benchmark_test.go \
  hat/hatPeer/compact_config_watch_test.go \
  hat/hatTopology/config_watch.go
