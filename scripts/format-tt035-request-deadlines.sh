#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	cmd/hatrie-cache/main.go \
	cmd/hatrie-cache/tt035_request_deadline_config_test.go \
	hat/hatCache/grpc.go \
	hat/hatCache/grpc_command_stream.go \
	hat/hatCache/monitoring.go \
	hat/hatCache/tt035_request_deadline_test.go \
	hat/hatCommand/request_deadline.go \
	hat/hatCommand/tt035_request_deadline_test.go
