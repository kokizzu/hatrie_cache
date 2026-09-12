#!/bin/sh
set -eu

gofmt -w \
	hat/hatPeer/compact_request_template.go \
	hat/hatPeer/compact_request_template_test.go \
	hat/hatPeer/compact_request_template_benchmark_test.go
