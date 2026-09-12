#!/bin/sh
set -eu

gofmt -w \
	cmd/hatrie-cache/main.go \
	cmd/hatrie-cache/tt043_maintenance_read_only_test.go \
	hat/hatCache/grpc.go \
	hat/hatCache/maintenance_read_only.go \
	hat/hatCache/monitoring.go \
	hat/hatCache/tt043_maintenance_read_only_test.go
