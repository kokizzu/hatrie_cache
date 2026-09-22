#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPeer/t238_batch_protocol_test.go hat/hatPeer/t238_batch_protocol_benchmark_test.go hat/hatPeer/compact_session.go
