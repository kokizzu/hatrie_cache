#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatPeer/request_authorization.go \
	hat/hatPeer/compact_session.go \
	hat/hatPeer/t241_authorization_baseline_benchmark_test.go \
	hat/hatPeer/t241_request_authorization_test.go
