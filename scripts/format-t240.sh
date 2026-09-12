#!/bin/sh
set -eu

gofmt -w hat/hatPeer/compact_session.go hat/hatPeer/request_cancellation_test.go
