#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPeer/compact_protocol.go hat/hatPeer/compact_request_template.go hat/hatPeer/compact_listener.go hat/hatPeer/compact_session.go hat/hatPeer/tr043c_response_schema_test.go hat/hatPeer/tr043c_response_schema_benchmark_test.go
