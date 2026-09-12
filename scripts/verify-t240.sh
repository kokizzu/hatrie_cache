#!/bin/sh
set -eu

gofmt -d hat/hatPeer/compact_session.go hat/hatPeer/request_cancellation_test.go hat/hatPeer/request_cancellation_benchmark_test.go
