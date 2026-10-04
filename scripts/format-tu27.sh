#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPeer/peer_config_watch.go hat/hatPeer/tu27_peer_config_watch_test.go hat/hatPeer/tu27_peer_config_watch_benchmark_test.go
