#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatPeer/connection_pool_lifecycle.go \
	hat/hatPeer/connection_pool_cancellation_test.go \
	hat/hatPeer/compact_session.go \
	hat/hatPeer/tu47_context_write_test.go
