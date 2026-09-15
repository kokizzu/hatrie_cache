#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPeer/compact_protocol.go hat/hatPeer/compact_protocol_test.go hat/hatPeer/compact_protocol_benchmark_test.go hat/hatPeer/compact_session.go hat/hatPeer/compact_session_test.go hat/hatPeer/compact_session_benchmark_test.go
