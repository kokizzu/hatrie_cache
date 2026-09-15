#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPeer/compact_listener.go hat/hatPeer/compact_listener_test.go hat/hatPeer/compact_listener_benchmark_test.go hat/hatPeer/compact_session.go
